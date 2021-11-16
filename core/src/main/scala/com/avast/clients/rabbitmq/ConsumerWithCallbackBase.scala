package com.avast.clients.rabbitmq

import java.time.{Duration, Instant}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Effect, Sync}
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.JavaConverters._
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.metrics.scalaapi._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{DefaultConsumer, ShutdownSignalException}

import scala.jdk.CollectionConverters._
import scala.language.higherKinds
import scala.util.control.NonFatal

abstract class ConsumerWithCallbackBase[F[_]: Effect](channel: ServerChannel,
                                                      failureAction: DeliveryResult,
                                                      consumerListener: ConsumerListener)
    extends DefaultConsumer(channel)
    with ConsumerBase[F] {

  override protected implicit val F: Sync[F] = Effect[F]

  protected val readMeter: Meter = monitor.meter("read")

  protected val processingFailedMeter: Meter = resultsMonitor.meter("processingFailed")

  protected val tasksMonitor: Monitor = monitor.named("tasks")

  protected val processingCount: AtomicInteger = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  protected val processedTimer: TimerPair = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, name, consumerTag, sig)

  protected def handleDelivery(messageId: MessageId,
                               correlationId: CorrelationId,
                               deliveryTag: DeliveryTag,
                               properties: BasicProperties,
                               routingKey: RoutingKey,
                               body: Array[Byte])(readAction: DeliveryReadAction[F, Bytes]): F[Unit] =
    F.delay {
      try {
        readMeter.mark()

        logger.debug(s"[$name] Read delivery with $messageId/$correlationId, $deliveryTag")

        val delivery = Delivery(Bytes.copyFrom(body), properties.asScala, Option(routingKey.value).getOrElse(""))

        logger.trace(s"[$name] Received delivery: $delivery")

        val st = Instant.now()

        @inline
        def taskDuration: Duration = Duration.between(st, Instant.now())

        unsafeExecuteReadAction(delivery, messageId, correlationId, deliveryTag, properties, routingKey, body, readAction, taskDuration _)
      } catch {
        // we catch this specific exception, handling of others is up to Lyra
        case e: RejectedExecutionException =>
          logger.debug(s"[$name] Executor was unable to plan the handling task for $messageId/$correlationId, $deliveryTag", e)
          handleFailure(messageId, correlationId, deliveryTag, properties, routingKey, body, e)

        case NonFatal(e) =>
          logger.error(
            s"[$name] Error while preparing callback execution for delivery with routing key $routingKey. This is probably a bug as the F construction shouldn't throw any exception",
            e
          )
          handleFailure(messageId, correlationId, deliveryTag, properties, routingKey, body, e)
      }
    }.flatten

  private def unsafeExecuteReadAction(delivery: Delivery.Ok[Bytes],
                                      messageId: MessageId,
                                      correlationId: CorrelationId,
                                      deliveryTag: DeliveryTag,
                                      properties: BasicProperties,
                                      routingKey: RoutingKey,
                                      body: Array[Byte],
                                      readAction: DeliveryReadAction[F, Bytes],
                                      taskDuration: () => Duration): F[Unit] = {
    readAction(delivery)
      .flatMap {
        handleResult(messageId, correlationId, deliveryTag, properties, routingKey, body)
      }
      .flatTap(_ =>
        F.delay {
          val duration = taskDuration()
          logger.debug(s"[$name] Delivery $messageId/$correlationId handling succeeded in $duration")
          processedTimer.update(duration)
      })
      .recoverWith {
        case NonFatal(t) =>
          F.delay {
            val duration = taskDuration()
            logger.debug(s"[$name] Delivery $messageId/$correlationId handling failed in $duration", t)
            processedTimer.updateFailure(duration)
            logger.error(s"[$name] Error while executing callback for delivery $messageId/$correlationId with $routingKey", t)
          } >>
            handleFailure(messageId, correlationId, deliveryTag, properties, routingKey, body, t)
      }
  }

  private def handleFailure(messageId: MessageId,
                            correlationId: CorrelationId,
                            deliveryTag: DeliveryTag,
                            properties: BasicProperties,
                            routingKey: RoutingKey,
                            body: Array[Byte],
                            t: Throwable): F[Unit] = {
    F.delay {
      processingCount.decrementAndGet()
      processingFailedMeter.mark()
      consumerListener.onError(this, name, channel, t)
    } >>
      executeFailureAction(messageId, correlationId, deliveryTag, properties, routingKey, body)
  }

  private def executeFailureAction(messageId: MessageId,
                                   correlationId: CorrelationId,
                                   deliveryTag: DeliveryTag,
                                   properties: BasicProperties,
                                   routingKey: RoutingKey,
                                   body: Array[Byte]): F[Unit] = {
    handleResult(messageId, correlationId, deliveryTag, properties, routingKey, body)(failureAction)
  }
}
