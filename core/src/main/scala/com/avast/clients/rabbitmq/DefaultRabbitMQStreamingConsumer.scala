package com.avast.clients.rabbitmq

import cats.effect.concurrent._
import cats.effect.implicits.toConcurrentOps
import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, Resource, Timer}
import cats.syntax.all._
import com.avast.clients.rabbitmq.DefaultRabbitMQStreamingConsumer.DeliveryQueue
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{Delivery => _, _}
import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}
import org.slf4j.event.Level

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

class DefaultRabbitMQStreamingConsumer[F[_]: ConcurrentEffect: Timer, A] private (
    base: ConsumerBase[F, A],
    channelOpsFactory: ConsumerChannelOpsFactory[F, A],
    initialConsumerTag: String,
    consumerListener: ConsumerListener[F],
    processTimeout: FiniteDuration,
    timeoutAction: DeliveryResult,
    timeoutLogLevel: Level,
    recoveringMutex: Semaphore[F])(createQueue: F[DeliveryQueue[F, A]])
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
      channelOpsFactory.create.flatMap { channelOps =>
        Resource
          .make {
            F.defer {
              val newConsumer = new StreamingConsumer(channelOps, newQueue)
              consumer
                .getAndSet(Some(newConsumer))
                .flatMap { oc =>
                  val consumerTag = oc match {
                    case Some(oldConsumer) => oldConsumer.getConsumerTag
                    case None => initialConsumerTag
                  }

                  consumerLogger.plainDebug(s"[$consumerName] Starting consuming") >>
                    DefaultRabbitMQClientFactory.startConsumingQueue(channelOps.channel, queueName, consumerTag, newConsumer, blocker) >>
                    isOk.set(true)
                }
                .as(newConsumer)
            }
          } { oldConsumer =>
            val consumerTag = oldConsumer.getConsumerTag
            consumerLogger.plainDebug(s"[$consumerName] Cancelling consumer with consumer tag $consumerTag") >>
              oldConsumer.stopConsuming()
          }
      }
    }
  }

  private lazy val close: F[Unit] = recoveringMutex.withPermit {
    isOk.get.flatMap { isOk =>
      if (isOk) consumer.get.flatMap {
        case Some(streamingConsumer) => streamingConsumer.stopConsuming()
        case None => F.unit
      } else F.unit
    } >> isOk.set(false) >> isClosed.set(true)
  }

  private lazy val stopConsuming: F[Unit] = {
    recoveringMutex.withPermit {
      isOk.set(false)
      // the consumer is stopped by the Resource
    }
  }

  private lazy val checkNotClosed: F[Unit] = {
    isClosed.get.flatMap(cl => if (cl) F.raiseError[Unit](new IllegalStateException("This consumer is already closed")) else F.unit)
  }

  private def handleStreamFinalize(e: ExitCase[Throwable]): F[Unit] = e match {
    case ExitCase.Completed =>
      stopConsuming
        .flatTap(_ => consumerLogger.plainDebug(s"[$consumerName] Delivery stream was completed"))

    case ExitCase.Canceled =>
      stopConsuming
        .flatTap(_ => consumerLogger.plainDebug(s"[$consumerName] Delivery stream was cancelled"))

    case ExitCase.Error(e: ShutdownSignalException) =>
      stopConsuming
        .flatTap { _ =>
          F.delay { streamFailureMeter.mark() } >>
            consumerLogger.plainError(e)(
              s"[$consumerName] Delivery stream was terminated because of channel shutdown. It might be a BUG int the client")
        }

    case ExitCase.Error(e) =>
      stopConsuming
        .flatTap(
          _ =>
            F.delay {
              streamFailureMeter.mark()
            } >>
              consumerLogger.plainDebug(e)(s"[$consumerName] Delivery stream was terminated"))
  }

  private def enqueueDelivery(delivery: Delivery[A], deferred: Deferred[F, Either[Throwable, DeliveryResult]])
    : F[SignallingRef[F, Option[Deferred[F, Either[Throwable, DeliveryResult]]]]] = {
    for {
      consumerOpt <- recoveringMutex.withPermit { this.consumer.get }
      consumer = consumerOpt.getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
      ref <- SignallingRef(Option(deferred))
      streamedDelivery = createStreamedDelivery(delivery, ref)
      _ <- consumer.queue.enqueue1((streamedDelivery, ref))
    } yield {
      ref
    }
  }

  private class StreamingConsumer(channelOps: ConsumerChannelOps[F, A], val queue: DeliveryQueue[F, A])
      extends ConsumerWithCallbackBase(base, channelOps, DeliveryResult.Retry, consumerListener) {
    private val receivingEnabled = Ref.unsafe[F, Boolean](true)

    override protected def handleNewDelivery(d: DeliveryWithMetadata[A]): F[Option[DeliveryResult]] = {
      import d._
      import metadata._

      val ignoreDelivery: F[Option[DeliveryResult]] = consumerLogger
        .debug(
          s"[$consumerName] Delivery result for $messageId ($deliveryTag) was ignored because consumer the is not OK - it will be redelivered later")
        .as(None)

      receivingEnabled.get
        .flatMap {
          case true =>
            Deferred[F, Either[Throwable, DeliveryResult]]
              .flatMap { waitForResult(delivery, messageId, deliveryTag) }
              .flatMap { dr =>
                receivingEnabled.get.flatMap {
                  case false => ignoreDelivery
                  case true => F.pure(Some(dr))
                }
              }

          case false => ignoreDelivery
        }
    }

    def stopConsuming(): F[Unit] = {
      receivingEnabled.getAndSet(false).flatMap {
        case true =>
          consumerLogger.plainDebug(s"[$consumerName] Stopping consummation for $getConsumerTag") >>
            blocker.delay {
              channelOps.channel.basicCancel(getConsumerTag)
              channelOps.channel.setDefaultConsumer(null)
            }

        case false => consumerLogger.plainDebug(s"Can't stop consummation for $getConsumerTag because it's already stopped")
      }
    }
  }

  private def waitForResult(delivery: Delivery[A], messageId: MessageId, deliveryTag: DeliveryTag)(
      deferred: Deferred[F, Either[Throwable, DeliveryResult]])(implicit cid: CorrelationId): F[DeliveryResult] = {
    // we want just the enqueuing to be in the mutex!
    enqueueDelivery(delivery, deferred)
      .flatMap { ref =>
        val enqueueTime = Instant.now()

        val result = deferred.get.flatMap {
          case Right(r) => F.pure(r)
          case Left(err) =>
            consumerLogger.debug(err)(s"[$consumerName] Failure when processing delivery $messageId ($deliveryTag)") >>
              F.raiseError[DeliveryResult](err)
        }

        watchForTimeoutIfConfigured(processTimeout, timeoutAction, timeoutLogLevel)(delivery, messageId, result) {
          F.defer {
            val l = java.time.Duration.between(enqueueTime, Instant.now())
            consumerLogger.debug(s"[$consumerName] Timeout after $l, cancelling processing of $messageId ($deliveryTag)")
          } >> ref.set(None) // cancel by this!
        }
      }
  }
}

object DefaultRabbitMQStreamingConsumer {

  private type QueuedDelivery[F[_], A] = (StreamedDelivery[F, A], SignallingRef[F, Option[Deferred[F, Either[Throwable, DeliveryResult]]]])
  private type DeliveryQueue[F[_], A] = Queue[F, QueuedDelivery[F, A]]

  def make[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
      base: ConsumerBase[F, A],
      channelOpsFactory: ConsumerChannelOpsFactory[F, A],
      initialConsumerTag: String,
      consumerListener: ConsumerListener[F],
      queueBufferSize: Int,
      timeout: FiniteDuration,
      timeoutAction: DeliveryResult,
      timeoutLogLevel: Level)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    val newQueue: F[DeliveryQueue[F, A]] = createQueue(queueBufferSize)

    Resource.make(Semaphore[F](1).map { mutex =>
      new DefaultRabbitMQStreamingConsumer(
        base,
        channelOpsFactory,
        initialConsumerTag,
        consumerListener,
        timeout,
        timeoutAction,
        timeoutLogLevel,
        mutex,
      )(newQueue)
    })(_.close)
  }

  private def createQueue[F[_]: ConcurrentEffect, A](queueBufferSize: Int): F[DeliveryQueue[F, A]] = {
    fs2.concurrent.Queue.bounded[F, QueuedDelivery[F, A]](queueBufferSize)
  }
}
