package com.avast.clients.rabbitmq

import cats.effect.{ConcurrentEffect, Effect, Timer => CatsTimer}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.metrics.scalaapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

import java.time.{Duration, Instant}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

abstract class ConsumerWithCallbackBase[F[_]: ConcurrentEffect: CatsTimer, A: DeliveryConverter](base: ConsumerBase[F, A],
                                                                                                 failureAction: DeliveryResult,
                                                                                                 consumerListener: ConsumerListener)
    extends DefaultConsumer(base.channel) {
  import base._

  protected val readMeter: Meter = consumerRootMonitor.meter("read")

  protected val processingFailedMeter: Meter = resultsMonitor.meter("processingFailed")

  protected val tasksMonitor: Monitor = consumerRootMonitor.named("tasks")

  protected val processingCount: AtomicInteger = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  protected val processedTimer: TimerPair = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, consumerName, consumerTag, sig)

  protected def handleNewDelivery(d: DeliveryWithMetadata[A]): F[DeliveryResult]

  override final def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val action = F.delay(processingCount.incrementAndGet()) >> {
      val rawBody = Bytes.copyFrom(body)

      base
        .parseDelivery(envelope, rawBody, properties)
        .flatMap { d =>
          import d.delivery
          import d.metadata._

          try {
            readMeter.mark()

            consumerLogger.debug(s"[$consumerName] Read delivery with $messageId/$correlationId, $deliveryTag")
            consumerLogger.trace(s"[$consumerName] Received delivery: $delivery")

            val st = Instant.now()

            @inline
            val taskDuration = () => Duration.between(st, Instant.now())

            unsafeExecuteReadAction(d, fixedProperties, rawBody, taskDuration)
          } catch {
            // we catch this specific exception, handling of others is up to Lyra
            case e: RejectedExecutionException =>
              consumerLogger.debug(
                s"[$consumerName] Executor was unable to plan the handling task for $messageId/$correlationId, $deliveryTag",
                e)
              handleFailure(d, rawBody, e)

            case NonFatal(e) =>
              consumerLogger.error(
                s"[$consumerName] Error while preparing callback execution for delivery with routing key $routingKey. This is probably a bug as the F construction shouldn't throw any exception",
                e
              )
              handleFailure(d, rawBody, e)
          }
        }
        .recoverWith {
          case e =>
            F.delay {
              processingCount.decrementAndGet()
              processingFailedMeter.mark()
              consumerLogger.debug(s"Could not process delivery with delivery tag ${envelope.getDeliveryTag}", e)
            } >> F.raiseError[Unit](e)
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
        handleResult(messageId, correlationId, deliveryTag, properties, routingKey, rawBody, delivery.delivery)
      }
      .flatTap(_ =>
        F.delay {
          val duration = taskDuration()
          consumerLogger.debug(s"[$consumerName] Delivery $messageId/$correlationId handling succeeded in $duration")
          processedTimer.update(duration)
      })
      .recoverWith {
        case NonFatal(t) =>
          F.delay {
            val duration = taskDuration()
            consumerLogger.debug(s"[$consumerName] Delivery $messageId/$correlationId handling failed in $duration", t)
            processedTimer.updateFailure(duration)
            consumerLogger
              .error(s"[$consumerName] Error while executing callback for delivery $messageId/$correlationId with $routingKey", t)
          } >>
            handleFailure(delivery, rawBody, t)
      }
  }

  private def handleFailure(delivery: DeliveryWithMetadata[A], rawBody: Bytes, t: Throwable): F[Unit] = {
    F.delay {
      processingCount.decrementAndGet()
      processingFailedMeter.mark()
      consumerListener.onError(this, consumerName, channel, t)
    } >>
      executeFailureAction(delivery, rawBody)
  }

  private def executeFailureAction(d: DeliveryWithMetadata[A], rawBody: Bytes): F[Unit] = {
    import d._
    import metadata._
    handleResult(messageId, correlationId, deliveryTag, fixedProperties, routingKey, rawBody, delivery)(failureAction)
  }
}
