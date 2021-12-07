package com.avast.clients.rabbitmq

import cats.effect.concurrent._
import cats.effect.implicits.toConcurrentOps
import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, Resource, Timer}
import cats.syntax.all._
import com.avast.clients.rabbitmq.DefaultRabbitMQStreamingConsumer.DeliveryQueue
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{Delivery => _, _}
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}
import org.slf4j.event.Level

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class DefaultRabbitMQStreamingConsumer[F[_]: ConcurrentEffect: Timer, A] private (
    base: ConsumerBase[F, A],
    initialConsumerTag: String,
    consumerListener: ConsumerListener,
    processTimeout: FiniteDuration,
    timeoutAction: DeliveryResult,
    timeoutLogLevel: Level,
    recoveringMutex: Semaphore[F])(createQueue: F[DeliveryQueue[F, A]], newChannel: Resource[F, ServerChannel])
    extends RabbitMQStreamingConsumer[F, A] {
  import base._

  private lazy val streamFailureMeter = consumerRootMonitor.meter("streamFailures")

  private lazy val consumer = Ref.unsafe[F, Option[StreamingConsumer]](None)
  private lazy val isClosed = Ref.unsafe[F, Boolean](false)
  private lazy val isOk = Ref.unsafe[F, Boolean](false)

  lazy val deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]] = {
    Stream
      .resource(Resource.eval(checkNotClosed) >> recoverIfNeeded)
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

  private def createStreamedDelivery(
      delivery: Delivery[A],
      deffRef: SignallingRef[F, Option[Deferred[F, Either[Throwable, DeliveryResult]]]]): StreamedDelivery[F, A] = {
    (f: DeliveryReadAction[F, A]) =>
      deffRef.get.flatMap {
        case Some(deff) =>
          f(delivery).start.flatMap { fiber =>
            val waitForCancel: F[Unit] = deffRef.discrete
              .collect {
                // We don't care about Some. None means cancel; Some appears only "sometimes" as the initial value update.
                case None => fiber.cancel
              }
              .take(1) // wait for a single (first) update
              .compile
              .last
              .map(_.getOrElse(throw new IllegalStateException("This must not happen!")))

            val waitForFinish = fiber.join.attempt.flatMap(deff.complete)

            (waitForCancel race waitForFinish).as(())
          }

        case None => F.unit // we're not starting the task
      }
  }

  private lazy val recoverIfNeeded: Resource[F, DeliveryQueue[F, A]] = {
    Resource(recoveringMutex.withPermit {
      Resource
        .eval(isOk.get)
        .flatMap {
          case true => Resource.eval(getCurrentQueue)
          case false => recover
        }
        .allocated // this is plumbing... we need to _stick_ the resource through plain F here, because of the mutex
    })
  }

  private lazy val getCurrentQueue: F[DeliveryQueue[F, A]] = {
    consumer.get.map { cons =>
      cons
        .getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
        .queue
    }
  }

  private lazy val recover: Resource[F, DeliveryQueue[F, A]] = {
    Resource.eval(createQueue).flatTap { newQueue =>
      newChannel.evalMap { newChannel =>
        val newConsumer = new StreamingConsumer(base.withNewChannel(newChannel), newQueue)

        consumer
          .getAndSet(Some(newConsumer))
          .flatMap {
            case Some(oldConsumer) =>
              blocker
                .delay {
                  val consumerTag = oldConsumer.getConsumerTag
                  logger.debug(s"[$consumerName] Cancelling consumer with consumer tag $consumerTag")

                  try {
                    oldConsumer.channel.close()
                  } catch {
                    case NonFatal(e) => logger.debug(s"[$consumerName] Could not close channel", e)
                  }

                  consumerTag
                }

            case None =>
              F.delay {
                logger.debug(s"[$consumerName] No old consumer to be cancelled")
                initialConsumerTag
              }
          }
          .flatMap { consumerTag =>
            blocker.delay {
              logger.debug(s"[$consumerName] Starting consuming")
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
        .flatTap(_ => F.delay(logger.debug(s"[$consumerName] Delivery stream was completed")))

    case ExitCase.Canceled =>
      stopConsuming
        .flatTap(_ => F.delay(logger.debug(s"[$consumerName] Delivery stream was cancelled")))

    case ExitCase.Error(e: ShutdownSignalException) =>
      stopConsuming
        .flatTap { _ =>
          F.delay {
            streamFailureMeter.mark()
            logger.error(s"[$consumerName] Delivery stream was terminated because of channel shutdown. It might be a BUG int the client", e)
          }
        }

    case ExitCase.Error(e) =>
      stopConsuming
        .flatTap(_ =>
          F.delay {
            streamFailureMeter.mark()
            logger.debug(s"[$consumerName] Delivery stream was terminated", e)
        })
  }

  private val timeoutDelivery = watchForTimeoutIfConfigured(processTimeout, timeoutAction, timeoutLogLevel) _

  private def enqueueDelivery(delivery: Delivery[A], deferred: Deferred[F, Either[Throwable, DeliveryResult]])
    : F[SignallingRef[F, Option[Deferred[F, Either[Throwable, DeliveryResult]]]]] = {
    for {
      consumerOpt <- this.consumer.get
      consumer = consumerOpt.getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
      ref <- SignallingRef(Option(deferred))
      streamedDelivery = createStreamedDelivery(delivery, ref)
      _ <- consumer.queue.enqueue1((streamedDelivery, ref))
    } yield {
      ref
    }
  }

  private class StreamingConsumer(base: ConsumerBase[F, A], val queue: DeliveryQueue[F, A])
      extends ConsumerWithCallbackBase(base, DeliveryResult.Retry, consumerListener) {
    import base._

    val channel: ServerChannel = base.channel

    private val receivingEnabled = Ref.unsafe[F, Boolean](true)

    override protected def handleNewDelivery(d: DeliveryWithMetadata[A]): F[DeliveryResult] = {
      import d._
      import metadata._

      receivingEnabled.get
        .flatMap {
          case false =>
            F.delay {
              logger.trace(
                s"[$consumerName] Delivery $messageId/$correlationId ($deliveryTag) was ignored because consumer is not OK - it will be redelivered later")
              DeliveryResult.Retry
            }

          case true =>
            Deferred[F, Either[Throwable, DeliveryResult]].flatMap { deferred =>
              // we want just the enqueuing to be in the mutex!
              recoveringMutex
                .withPermit {
                  enqueueDelivery(delivery, deferred)
                }
                .flatMap { ref =>
                  val enqueueTime = Instant.now()

                  val result: F[DeliveryResult] = deferred.get.flatMap {
                    case Right(r) => F.pure(r)
                    case Left(err) =>
                      logger.debug(s"[$consumerName] Failure when processing delivery $messageId/$correlationId", err)
                      F.raiseError(err)
                  }

                  timeoutDelivery(delivery, messageId, correlationId, result) {
                    F.delay {
                      val l = java.time.Duration.between(enqueueTime, Instant.now())
                      logger.debug(s"[$consumerName] Timeout after being $l in queue, cancelling processing of $messageId/$correlationId")
                    } >> ref.set(None) // cancel by this!
                  }
                }
            }
        }
    }

    def stopConsuming(): F[Unit] = {
      receivingEnabled.set(false) >> blocker.delay {
        logger.debug(s"[$consumerName] Stopping consummation for $getConsumerTag")
        channel.basicCancel(getConsumerTag)
        channel.setDefaultConsumer(null)
      }
    }
  }
}

object DefaultRabbitMQStreamingConsumer extends StrictLogging {

  private type QueuedDelivery[F[_], A] = (StreamedDelivery[F, A], SignallingRef[F, Option[Deferred[F, Either[Throwable, DeliveryResult]]]])
  private type DeliveryQueue[F[_], A] = Queue[F, QueuedDelivery[F, A]]

  def make[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
      base: ConsumerBase[F, A],
      newChannel: Resource[F, ServerChannel],
      initialConsumerTag: String,
      consumerListener: ConsumerListener,
      queueBufferSize: Int,
      timeout: FiniteDuration,
      timeoutAction: DeliveryResult,
      timeoutLogLevel: Level)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    val newQueue: F[DeliveryQueue[F, A]] = createQueue(queueBufferSize)

    Resource.make(Semaphore[F](1).map { mutex =>
      new DefaultRabbitMQStreamingConsumer(
        base,
        initialConsumerTag,
        consumerListener,
        timeout,
        timeoutAction,
        timeoutLogLevel,
        mutex,
      )(newQueue, newChannel)
    })(_.close)
  }

  private def createQueue[F[_]: ConcurrentEffect, A](queueBufferSize: Int): F[DeliveryQueue[F, A]] = {
    fs2.concurrent.Queue.bounded[F, QueuedDelivery[F, A]](queueBufferSize)
  }
}
