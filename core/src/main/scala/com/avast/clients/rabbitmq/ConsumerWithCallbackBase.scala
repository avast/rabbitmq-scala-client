package com.avast.clients.rabbitmq

import cats.effect.{ConcurrentEffect, Timer => CatsTimer}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.metrics.scalaeffectapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import java.time.{Duration, Instant}
import java.util.concurrent.RejectedExecutionException
import scala.util.control.NonFatal

abstract class ConsumerWithCallbackBase[F[_]: ConcurrentEffect: CatsTimer, A: DeliveryConverter](base: ConsumerBase[F, A],
                                                                                                 channelOps: ConsumerChannelOps[F, A],
                                                                                                 failureAction: DeliveryResult,
                                                                                                 consumerListener: ConsumerListener[F])
    extends DefaultConsumer(channelOps.channel) {
  import base._
  import channelOps._

  protected val readMeter: Meter[F] = consumerRootMonitor.meter("read")

  protected val processingFailedMeter: Meter[F] = resultsMonitor.meter("processingFailed")

  protected val tasksMonitor: Monitor[F] = consumerRootMonitor.named("tasks")

  protected val processingCount: SettableGauge[F, Long] = tasksMonitor.gauge.settableLong("processing", replaceExisting = true)

  protected val processedTimer: TimerPair[F] = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {
    consumerListener.onShutdown(this, channel, consumerName, consumerTag, sig).unsafeStartAndForget()
  }

  protected def handleNewDelivery(d: DeliveryWithContext[A]): F[Option[ConfirmedDeliveryResult[F]]]

  override final def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val action = processingCount.inc >> {
      val rawBody = Bytes.copyFrom(body)

      base
        .parseDelivery(envelope, rawBody, properties)
        .flatMap { d =>
          import d.context
          import context._

          consumerLogger.debug(s"[$consumerName] Read delivery with $messageId, $deliveryTag") >>
            consumerLogger.trace(s"[$consumerName] Received delivery: ${d.delivery}") >>
            readMeter.mark >> {

            val st = Instant.now()
            val taskDuration = F.delay(Duration.between(st, Instant.now()))

            unsafeExecuteReadAction(d, rawBody, taskDuration)
              .recoverWith {
                case e: RejectedExecutionException =>
                  consumerLogger.debug(e)(s"[$consumerName] Executor was unable to plan the handling task for $messageId, $deliveryTag") >>
                    handleFailure(d, rawBody, e)
              }
          }
        }
        .recoverWith {
          case e =>
            processingCount.dec >>
              processingFailedMeter.mark >>
              consumerLogger.plainDebug(e)(s"Could not process delivery with delivery tag ${envelope.getDeliveryTag}") >>
              F.raiseError[Unit](e)
        }
    }

    // Actually start the processing. This is the barrier between synchronous and asynchronous world.
    startAndForget {
      action
    }
  }

  private def unsafeExecuteReadAction(delivery: DeliveryWithContext[A], rawBody: Bytes, taskDuration: F[Duration]): F[Unit] = {
    import delivery.context
    import context._

    handleNewDelivery(delivery)
      .flatMap {
        case Some(dr) =>
          handleResult(rawBody, delivery.delivery)(dr.deliveryResult) >> dr.confirm

        case None =>
          consumerLogger.trace(s"[$consumerName] Delivery result for $messageId ignored")
      }
      .flatTap { _ =>
        taskDuration.flatMap { duration =>
          consumerLogger.debug(s"[$consumerName] Delivery $messageId handling succeeded in $duration") >>
            processedTimer.update(duration) >>
            processingCount.dec
        }
      }
      .recoverWith {
        case NonFatal(t) =>
          taskDuration.flatMap { duration =>
            consumerLogger.debug(t)(s"[$consumerName] Delivery $messageId handling failed in $duration") >>
              processedTimer.updateFailure(duration) >>
              handleFailure(delivery, rawBody, t)
          }
      }
  }

  private def handleFailure(delivery: DeliveryWithContext[A], rawBody: Bytes, t: Throwable)(implicit dctx: DeliveryContext): F[Unit] = {
    processingFailedMeter.mark >>
      processingCount.dec >>
      consumerListener.onError(this, consumerName, channel, t) >>
      executeFailureAction(delivery, rawBody)
  }

  private def executeFailureAction(d: DeliveryWithContext[A], rawBody: Bytes): F[Unit] = {
    import d._
    handleResult(rawBody, delivery)(failureAction)
  }
}
