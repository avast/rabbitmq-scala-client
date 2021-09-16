package com.avast.clients.rabbitmq

import cats.effect.concurrent._
import cats.effect.{Blocker, CancelToken, Concurrent, ConcurrentEffect, ContextShift, Effect, ExitCase, IO, Resource, Sync, Timer}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQStreamingConsumer.DeliveryQueue
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.{Delivery => _, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream
import fs2.concurrent.Queue

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
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
    timeoutAction: (Delivery[Bytes], TimeoutException) => F[DeliveryResult],
    recoveringMutex: Semaphore[F],
    blocker: Blocker)(createQueue: F[DeliveryQueue[F, Bytes]], newChannel: F[ServerChannel])(implicit cs: ContextShift[F])
    extends RabbitMQStreamingConsumer[F, A]
    with StrictLogging {

  private lazy val F: Sync[F] = Sync[F]

  private lazy val streamFailureMeter = monitor.meter("streamFailures")

  private lazy val consumer = Ref.unsafe[F, Option[StreamingConsumer]](None)
  private lazy val isClosed = Ref.unsafe[F, Boolean](false)
  private lazy val isOk = Ref.unsafe[F, Boolean](false)
  private lazy val tasks = Ref.unsafe[IO, Set[CancelToken[IO]]](Set.empty)

  lazy val deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]] = {
    Stream
      .eval { checkNotClosed >> recoverIfNeeded }
      .flatMap { queue =>
        queue.dequeue
          .map {
            case (del, deff) =>
              new StreamedDelivery[F, A] {
                override val delivery: Delivery[A] = DefaultRabbitMQClientFactory.convertDelivery(del)
                override def handle(result: DeliveryResult): F[StreamedResult] = deff.complete(result).map(_ => StreamedResult)
              }
          }
      }
      .onFinalizeCase(handleStreamFinalize)
  }

  private lazy val recoverIfNeeded: F[DeliveryQueue[F, Bytes]] = {
    recoveringMutex.withPermit {
      isOk.get.flatMap {
        case true => getCurrentQueue
        case false => recover
      }
    }
  }

  private lazy val getCurrentQueue: F[DeliveryQueue[F, Bytes]] = {
    consumer.get.map { cons =>
      cons
        .getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
        .queue
    }
  }

  private lazy val recover: F[DeliveryQueue[F, Bytes]] = {
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
                    case NonFatal(e) => logger.debug(s"Could not close channel", e)
                  }

                  consumerTag
                }
                .flatTap(_ => tryCancelRunningTasks)

            case None =>
              F.delay {
                logger.debug("No old consumer to be cancelled!")
                initialConsumerTag
              }
          }
          .flatMap { consumerTag =>
            blocker.delay {
              logger.debug("Starting consuming")
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

  private def deliveryCallback(delivery: Delivery[Bytes]): F[DeliveryResult] = {
    for {
      deferred <- Deferred[F, DeliveryResult]
      consumerOpt <- this.consumer.get
      consumer = consumerOpt.getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
      _ <- consumer.queue.enqueue1((delivery, deferred))
      res <- Concurrent.timeout(deferred.get, timeout).recoverWith { case e: TimeoutException => timeoutAction(delivery, e) }
    } yield {
      res
    }
  }

  private class StreamingConsumer(override val channel: ServerChannel, val queue: DeliveryQueue[F, Bytes])
      extends ConsumerWithCallbackBase(channel, DeliveryResult.Retry, consumerListener) {
    private val receivingEnabled = Ref.unsafe[F, Boolean](true)
    override protected val republishStrategy: RepublishStrategy = DefaultRabbitMQStreamingConsumer.this.republishStrategy

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
      processingCount.incrementAndGet()

      val deliveryTag = envelope.getDeliveryTag
      val messageId = properties.getMessageId
      val routingKey = properties.getOriginalRoutingKey.getOrElse(envelope.getRoutingKey)

      val task: F[Unit] = receivingEnabled.get.flatMap {
        case false =>
          F.delay {
            logger.trace(s"Delivery $messageId (tag $deliveryTag) was ignored because consumer is not OK - it will be redelivered later")
            processingCount.decrementAndGet()
            ()
          }

        case true =>
          val action = handleDelivery(messageId, deliveryTag, properties, routingKey, body)(deliveryCallback)
            .flatTap(_ => F.delay(logger.debug(s"Delivery $messageId processed successfully (tag $deliveryTag)")))
            .recoverWith {
              case e =>
                F.delay {
                  processingFailedMeter.mark()
                  logger.debug("Could not process delivery", e)
                } >> F.raiseError(e)
            }

          recoveringMutex.withPermit(F.suspend {
            lazy val ct: CancelToken[IO] = Effect[F]
              .toIO(action)
              .runCancelable(_ => {
                processingCount.decrementAndGet()
                tasks.update(_ - ct)
              })
              .unsafeRunSync()

            tasks.update(_ + ct).to[F]
          })
      }

      Effect[F].toIO(task).unsafeToFuture()

      ()
    }

    def stopConsuming(): F[Unit] = {
      receivingEnabled.set(false) >> blocker.delay {
        logger.debug(s"Stopping consummation for $getConsumerTag")
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

  private type DeliveryQueue[F[_], A] = Queue[F, (Delivery[A], Deferred[F, DeliveryResult])]

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
      timeoutAction: (Delivery[Bytes], TimeoutException) => F[DeliveryResult],
      blocker: Blocker)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    val newQueue: F[DeliveryQueue[F, Bytes]] = createQueue(queueBufferSize)

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
                                           blocker)(newQueue, newChannel)
    })(_.close)
  }

  private def createQueue[F[_]: ConcurrentEffect](queueBufferSize: Int): F[DeliveryQueue[F, Bytes]] = {
    fs2.concurrent.Queue.bounded[F, (Delivery[Bytes], Deferred[F, DeliveryResult])](queueBufferSize)
  }
}
