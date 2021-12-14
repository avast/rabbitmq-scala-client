package com.avast.clients.rabbitmq

import cats.effect.{ConcurrentEffect, Effect, Timer => CatsTimer}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{CorrelationId, DeliveryResult}
import com.avast.metrics.scalaapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import java.time.{Duration, Instant}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

abstract class ConsumerWithCallbackBase[F[_]: ConcurrentEffect: CatsTimer, A: DeliveryConverter](base: ConsumerBase[F, A],
                                                                                                 channelOps: ConsumerChannelOps[F, A],
                                                                                                 failureAction: DeliveryResult,
                                                                                                 consumerListener: ConsumerListener[F])
    extends DefaultConsumer(channelOps.channel) {
  import base._
  import channelOps._

  protected val readMeter: Meter = consumerRootMonitor.meter("read")

  protected val processingFailedMeter: Meter = resultsMonitor.meter("processingFailed")

  protected val tasksMonitor: Monitor = consumerRootMonitor.named("tasks")

  protected val processingCount: AtomicInteger = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  protected val processedTimer: TimerPair = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, consumerName, consumerTag, sig)

  protected def handleNewDelivery(d: DeliveryWithMetadata[A]): F[Option[DeliveryResult]]

  override final def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val action = F.delay(processingCount.incrementAndGet()) >> {
      val rawBody = Bytes.copyFrom(body)

      base
        .parseDelivery(envelope, rawBody, properties)
        .flatMap { d =>
          import d.delivery
          import d.metadata._

          consumerLogger.debug(s"[$consumerName] Read delivery with $messageId, $deliveryTag") >>
            consumerLogger.trace(s"[$consumerName] Received delivery: $delivery") >> {
            readMeter.mark()

            val st = Instant.now()

            @inline
            val taskDuration = () => Duration.between(st, Instant.now())

            unsafeExecuteReadAction(d, fixedProperties, rawBody, taskDuration)
              .recoverWith {
                case e: RejectedExecutionException =>
                  consumerLogger.debug(e)(s"[$consumerName] Executor was unable to plan the handling task for $messageId, $deliveryTag") >>
                    handleFailure(d, rawBody, e)
              }
          }
        }
        .recoverWith {
          case e =>
            F.delay {
              processingCount.decrementAndGet()
              processingFailedMeter.mark()
            } >>
              consumerLogger.plainDebug(e)(s"Could not process delivery with delivery tag ${envelope.getDeliveryTag}") >>
              F.raiseError[Unit](e)
        }
    }

    Effect[F].toIO(action).unsafeToFuture() // actually start the processing

    ()
  }

  private def unsafeExecuteReadAction(delivery: DeliveryWithMetadata[A],
                                      properties: BasicProperties,
                                      rawBody: Bytes,
                                      taskDuration: () => Duration): F[Unit] = {
    import delivery.metadata._

    handleNewDelivery(delivery)
      .flatMap {
        case Some(dr) => handleResult(messageId, deliveryTag, properties, routingKey, rawBody, delivery.delivery)(dr)
        case None => consumerLogger.trace(s"[$consumerName] Delivery result for $messageId ignored")
      }
      .flatTap(_ =>
        F.delay {
          val duration = taskDuration()
          consumerLogger.debug(s"[$consumerName] Delivery $messageId handling succeeded in $duration")
          processedTimer.update(duration)
          processingCount.decrementAndGet()
      })
      .recoverWith {
        case NonFatal(t) =>
          val duration = taskDuration()
          consumerLogger.debug(t)(s"[$consumerName] Delivery $messageId handling failed in $duration") >>
            F.delay { processedTimer.updateFailure(duration) } >>
            handleFailure(delivery, rawBody, t)
      }
  }

  private def handleFailure(delivery: DeliveryWithMetadata[A], rawBody: Bytes, t: Throwable)(
      implicit correlationId: CorrelationId): F[Unit] = {
    F.delay {
      processingCount.decrementAndGet()
      processingFailedMeter.mark()
    } >>
      consumerListener.onError(this, consumerName, channel, t) >>
      executeFailureAction(delivery, rawBody)
  }

  private def executeFailureAction(d: DeliveryWithMetadata[A], rawBody: Bytes): F[Unit] = {
    import d._
    import metadata._
    handleResult(messageId, deliveryTag, fixedProperties, routingKey, rawBody, delivery)(failureAction)
  }
}
