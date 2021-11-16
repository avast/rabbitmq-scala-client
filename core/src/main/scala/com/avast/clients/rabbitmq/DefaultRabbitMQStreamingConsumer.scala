package com.avast.clients.rabbitmq

import cats.effect.concurrent._
import cats.effect.implicits.toConcurrentOps
import cats.effect.{Blocker, CancelToken, Concurrent, ConcurrentEffect, ContextShift, Effect, ExitCase, IO, Resource, Sync, Timer}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQStreamingConsumer.DeliveryQueue
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.{Delivery => _, _}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}

import java.time.Instant
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class DefaultRabbitMQStreamingConsumer[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter] private (
    name: String,
    queueName: String,
    initialConsumerTag: String,
    connectionInfo: RabbitMQConnectionInfo,
    consumerListener: ConsumerListener,
    monitor: Monitor,
    republishStrategy: RepublishStrategy,
    timeout: FiniteDuration,
    timeoutAction: (Delivery[Bytes], MessageId, CorrelationId, Logger) => F[DeliveryResult],
    recoveringMutex: Semaphore[F],
    middlewares: List[RabbitMQStreamingConsumerMiddleware[F, A]],
    blocker: Blocker)(createQueue: F[DeliveryQueue[F, A]], newChannel: F[ServerChannel])(implicit cs: ContextShift[F])
    extends RabbitMQStreamingConsumer[F, A]
    with StrictLogging {

  private lazy val F: Sync[F] = Sync[F]

  private lazy val streamFailureMeter = monitor.meter("streamFailures")

  private lazy val consumer = Ref.unsafe[F, Option[StreamingConsumer]](None)
  private lazy val isClosed = Ref.unsafe[F, Boolean](false)
  private lazy val isOk = Ref.unsafe[F, Boolean](false)
  private lazy val tasks = Ref.unsafe[IO, Set[CancelToken[IO]]](Set.empty)

//  private val middlewares: List[StreamedConsumerMiddleware[F, A]] = List(
////    new StreamedConsumerMiddleware[F, A] {
////      override def adjustDelivery(d: StreamedDelivery[F, A]): F[StreamedDelivery[F, A]] = F.delay(println("adjustDelivery")).as(d)
////
////      override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = F.delay(println("adjustResult")) >> r
////    },
//    StreamingPoisonedMessageHandler(2)
//  )

  private lazy val finalMiddleware = new RabbitMQStreamingConsumerMiddleware[F, A] {
    override def adjustDelivery(d: StreamedDelivery[F, A]): F[StreamedDelivery[F, A]] = {
      middlewares.foldLeft(F.pure(d)) { case (prev, m) => prev.flatMap(sd => m.adjustDelivery(sd)) }
    }

    override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = {
      middlewares.foldLeft(r) { case (prev, m) => m.adjustResult(rawDelivery, prev) }
    }
  }

  lazy val deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]] = {
    Stream
      .eval { checkNotClosed >> recoverIfNeeded }
      .flatMap { queue =>
        queue.dequeue.evalMap {
          case (del, ref) =>
            ref.get.map(_.map { _ =>
              del
            })
        }.unNone
      }
      .onFinalizeCase(handleStreamFinalize)
  }

  private def createStreamedDelivery(delivery: Delivery[A],
                                     deffRef: SignallingRef[F, Option[Deferred[F, DeliveryResult]]]): StreamedDelivery[F, A] = {
    (f: DeliveryReadAction[F, A]) =>
      deffRef.get.flatMap {
        case Some(deff) =>
          f(delivery).start.flatMap { fiber =>
            val waitForCancel: F[Unit] = deffRef.discrete
              .collect {
                // We don't care about Some. None means cancel; Some appears only sometimes as the initial value update.
                case None => fiber.cancel
              }
              .take(1) // wait for a single (first) update
              .compile
              .last
              .map(_.getOrElse(throw new IllegalStateException("This must not happen!")))

            val waitForFinish = fiber.join.flatMap(deff.complete)

            (waitForCancel race waitForFinish).as(())
          }

        case None => F.unit // we're not starting the task
      }
  }

  private lazy val recoverIfNeeded: F[DeliveryQueue[F, A]] = {
    recoveringMutex.withPermit {
      isOk.get.flatMap {
        case true => getCurrentQueue
        case false => recover
      }
    }
  }

  private lazy val getCurrentQueue: F[DeliveryQueue[F, A]] = {
    consumer.get.map { cons =>
      cons
        .getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
        .queue
    }
  }

  private lazy val recover: F[DeliveryQueue[F, A]] = {
    createQueue.flatTap { newQueue =>
      newChannel.flatMap { newChannel =>
        val newConsumer = new StreamingConsumer(newChannel, newQueue)

        consumer
          .getAndSet(Some(newConsumer))
          .flatMap {
            case Some(oldConsumer) =>
              blocker
                .delay {
                  val consumerTag = oldConsumer.getConsumerTag
                  logger.debug(s"[$name] Cancelling consumer with consumer tag $consumerTag")

                  try {
                    oldConsumer.channel.close()
                  } catch {
                    case NonFatal(e) => logger.debug(s"[$name] Could not close channel", e)
                  }

                  consumerTag
                }
                .flatTap(_ => tryCancelRunningTasks)

            case None =>
              F.delay {
                logger.debug(s"[$name] No old consumer to be cancelled")
                initialConsumerTag
              }
          }
          .flatMap { consumerTag =>
            blocker.delay {
              logger.debug(s"[$name] Starting consuming")
              DefaultRabbitMQClientFactory.startConsumingQueue(newChannel, queueName, consumerTag, newConsumer)
              ()
            }
          } >> isOk.set(true)
      }
    }
  }

  private lazy val close: F[Unit] = recoveringMutex.withPermit {
    isOk.get.flatMap { isOk =>
      if (isOk) consumer.get.flatMap {
        case Some(streamingConsumer) =>
          blocker.delay {
            streamingConsumer.stopConsuming()
            streamingConsumer.channel.close()
          }

        case None => F.unit
      } else F.unit
    } >> isOk.set(false) >> isClosed.set(true)
  }

  private lazy val tryCancelRunningTasks: F[Unit] = {
    tasks
      .update { tasks =>
        tasks.foreach(_.unsafeRunSync())
        Set.empty
      }
      .to[F]
  }

  private lazy val stopConsuming: F[Unit] = {
    recoveringMutex.withPermit {
      isOk.set(false) >> consumer.get.flatMap(_.fold(F.unit)(_.stopConsuming())) // stop consumer, if there is some
    }
  }

  private lazy val checkNotClosed: F[Unit] = {
    isClosed.get.flatMap(cl => if (cl) F.raiseError[Unit](new IllegalStateException("This consumer is already closed")) else F.unit)
  }

  private def handleStreamFinalize(e: ExitCase[Throwable]): F[Unit] = e match {
    case ExitCase.Completed =>
      stopConsuming
        .flatTap(_ => F.delay(logger.debug(s"[$name] Delivery stream was completed")))

    case ExitCase.Canceled =>
      stopConsuming
        .flatTap(_ => F.delay(logger.debug(s"[$name] Delivery stream was cancelled")))

    case ExitCase.Error(e: ShutdownSignalException) =>
      stopConsuming
        .flatTap { _ =>
          F.delay {
            streamFailureMeter.mark()
            logger.error(s"[$name] Delivery stream was terminated because of channel shutdown. It might be a BUG int the client", e)
          }
        }

    case ExitCase.Error(e) =>
      stopConsuming
        .flatTap(_ =>
          F.delay {
            streamFailureMeter.mark()
            logger.debug(s"[$name] Delivery stream was terminated", e)
        })
  }

  private def deliveryCallback(messageId: MessageId, correlationId: CorrelationId)(delivery: Delivery[Bytes]): F[DeliveryResult] = {
    for {
      deferred <- Deferred[F, DeliveryResult]
      consumerOpt <- this.consumer.get
      consumer = consumerOpt.getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
      ref <- SignallingRef(Option(deferred))
      convertedDelivery = DefaultRabbitMQClientFactory.convertDelivery(delivery)
      streamedDelivery = createStreamedDelivery(convertedDelivery, ref)
      adjustedDelivery <- finalMiddleware.adjustDelivery(streamedDelivery)
      _ <- consumer.queue.enqueue1((adjustedDelivery, ref))
      enqueueTime = Instant.now()
      result = Concurrent.timeout(deferred.get, timeout).recoverWith {
        case _: TimeoutException =>
          val l = java.time.Duration.between(enqueueTime, Instant.now())

          F.delay { logger.debug(s"[$name] Timeout after being $l in queue, cancelling processing of $messageId/$correlationId") } >>
            ref.set(None) >> // try cancel the task
            timeoutAction(delivery, messageId, correlationId, logger)
      }
      adjustedResult <- finalMiddleware.adjustResult(convertedDelivery, result)
    } yield {
      adjustedResult
    }
  }

  private class StreamingConsumer(override val channel: ServerChannel, val queue: DeliveryQueue[F, A])
      extends ConsumerWithCallbackBase(channel, DeliveryResult.Retry, consumerListener) {
    private val receivingEnabled = Ref.unsafe[F, Boolean](true)
    override protected val republishStrategy: RepublishStrategy = DefaultRabbitMQStreamingConsumer.this.republishStrategy

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
      processingCount.incrementAndGet()

      val metadata = DeliveryMetadata.from(envelope, properties)
      import metadata._

      val task: F[Unit] = receivingEnabled.get
        .flatMap {
          case false =>
            F.delay {
              logger.trace(
                s"[$name] Delivery $messageId/$correlationId ($deliveryTag) was ignored because consumer is not OK - it will be redelivered later")
              processingCount.decrementAndGet()
              ()
            }

          case true =>
            recoveringMutex.withPermit(F.defer {
              lazy val ct: CancelToken[IO] = Effect[F]
                .toIO(createHandleAction(messageId, correlationId, deliveryTag, properties, routingKey, body))
                .runCancelable(_ => {
                  processingCount.decrementAndGet()
                  tasks.update(_ - ct)
                })
                .unsafeRunSync()

              tasks.update(_ + ct).to[F]
            })
        }
        .recoverWith {
          case e =>
            F.delay {
              logger.error(s"Error while handling incoming message $messageId/$correlationId", e)
            }
        }

      Effect[F].toIO(task).unsafeToFuture()

      ()
    }

    private def createHandleAction(messageId: MessageId,
                                   correlationId: CorrelationId,
                                   deliveryTag: DeliveryTag,
                                   properties: AMQP.BasicProperties,
                                   routingKey: RoutingKey,
                                   body: Array[Byte]): F[Unit] = {
      handleDelivery(messageId, correlationId, deliveryTag, properties, routingKey, body)(deliveryCallback(messageId, correlationId))
        .flatTap(_ => F.delay(logger.debug(s"[$name] Delivery $messageId/$correlationId processed successfully (tag $deliveryTag)")))
        .recoverWith {
          case e =>
            F.delay {
              processingFailedMeter.mark()
              logger.debug(s"[$name] Could not process delivery $messageId/$correlationId", e)
            } >> F.raiseError(e)
        }
    }

    def stopConsuming(): F[Unit] = {
      receivingEnabled.set(false) >> blocker.delay {
        logger.debug(s"[$name] Stopping consummation for $getConsumerTag")
        channel.basicCancel(getConsumerTag)
        channel.setDefaultConsumer(null)
      }
    }

    override protected implicit val cs: ContextShift[F] = DefaultRabbitMQStreamingConsumer.this.cs
    override protected def name: String = DefaultRabbitMQStreamingConsumer.this.name
    override protected def queueName: String = DefaultRabbitMQStreamingConsumer.this.queueName
    override protected def blocker: Blocker = DefaultRabbitMQStreamingConsumer.this.blocker
    override protected def connectionInfo: RabbitMQConnectionInfo = DefaultRabbitMQStreamingConsumer.this.connectionInfo
    override protected def monitor: Monitor = DefaultRabbitMQStreamingConsumer.this.monitor
  }
}

object DefaultRabbitMQStreamingConsumer extends StrictLogging {

  private type DeliveryQueue[F[_], A] = Queue[F, (StreamedDelivery[F, A], SignallingRef[F, Option[Deferred[F, DeliveryResult]]])]

  def make[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
      name: String,
      newChannel: F[ServerChannel],
      initialConsumerTag: String,
      queueName: String,
      connectionInfo: RabbitMQConnectionInfo,
      consumerListener: ConsumerListener,
      queueBufferSize: Int,
      monitor: Monitor,
      republishStrategy: RepublishStrategy,
      timeout: FiniteDuration,
      timeoutAction: (Delivery[Bytes], MessageId, CorrelationId, Logger) => F[DeliveryResult],
      middlewares: List[RabbitMQStreamingConsumerMiddleware[F, A]],
      blocker: Blocker)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    val newQueue: F[DeliveryQueue[F, A]] = createQueue(queueBufferSize)

    Resource.make(Semaphore[F](1).map { mutex =>
      new DefaultRabbitMQStreamingConsumer(name,
                                           queueName,
                                           initialConsumerTag,
                                           connectionInfo,
                                           consumerListener,
                                           monitor,
                                           republishStrategy,
                                           timeout,
                                           timeoutAction,
                                           mutex,
                                           middlewares,
                                           blocker)(newQueue, newChannel)
    })(_.close)
  }

  private def createQueue[F[_]: ConcurrentEffect, A](queueBufferSize: Int): F[DeliveryQueue[F, A]] = {
    fs2.concurrent.Queue.bounded[F, (StreamedDelivery[F, A], SignallingRef[F, Option[Deferred[F, DeliveryResult]]])](queueBufferSize)
  }
}

//private[rabbitmq] abstract class AbstractStreamedDelivery[F[_], A](implicit F: Sync[F]) extends StreamedDelivery[F, A] {
//  private val pipeline = Ref.unsafe(List.empty[(Delivery[A], DeliveryResult) => F[DeliveryResult]])
//
//  override private[rabbitmq] def addResultAdjustment(f: (Delivery[A], DeliveryResult) => F[DeliveryResult]): F[Unit] = {
//    pipeline.modify { l =>
//      (l :+ f, ())
//    }
//  }
//
//  override private[rabbitmq] final def adjustResult(delivery: Delivery[A], r: DeliveryResult): F[DeliveryResult] = {
//    pipeline.get.flatMap { l =>
//      l.foldLeft(F.pure(r)) { case (prev, f) => prev.flatMap(f(delivery, _)) }
//    }
//  }
//}
