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

  protected def handleDelivery(messageId: String, deliveryTag: Long, properties: BasicProperties, routingKey: String, body: Array[Byte])(
      readAction: DeliveryReadAction[F, Bytes]): F[Unit] =
    F.delay {
      try {
        readMeter.mark()

        logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

        val delivery = Delivery(Bytes.copyFrom(body), properties.asScala, Option(routingKey).getOrElse(""))

        logger.trace(s"[$name] Received delivery: $delivery")

        val st = Instant.now()

        @inline
        def taskDuration: Duration = Duration.between(st, Instant.now())

        readAction(delivery)
          .flatMap { handleResult(messageId, deliveryTag, properties, routingKey, body) }
          .flatTap(_ =>
            F.delay {
              val duration = taskDuration
              logger.debug(s"[$name] Delivery ID $messageId handling succeeded in $duration")
              processedTimer.update(duration)
          })
          .recoverWith {
            case NonFatal(t) =>
              F.delay {
                val duration = taskDuration
                logger.debug(s"[$name] Delivery ID $messageId handling failed in $duration", t)
                processedTimer.updateFailure(duration)
              } >>
                handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)(t)
          }
      } catch {
        // we catch this specific exception, handling of others is up to Lyra
        case e: RejectedExecutionException =>
          logger.debug(s"[$name] Executor was unable to plan the handling task", e)
          handleFailure(messageId, deliveryTag, properties, routingKey, body, e)

        case NonFatal(e) => handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)(e)
      }
    }.flatten

  private def handleCallbackFailure(messageId: String,
                                    deliveryTag: Long,
                                    properties: BasicProperties,
                                    routingKey: String,
                                    body: Array[Byte])(t: Throwable): F[Unit] = {
    F.delay {
      logger.error(s"[$name] Error while executing callback, it's probably a BUG", t)
    } >>
      handleFailure(messageId, deliveryTag, properties, routingKey, body, t)
  }

  private def handleFailure(messageId: String,
                            deliveryTag: Long,
                            properties: BasicProperties,
                            routingKey: String,
                            body: Array[Byte],
                            t: Throwable): F[Unit] = {
    F.delay {
      processingCount.decrementAndGet()
      processingFailedMeter.mark()
      consumerListener.onError(this, name, channel, t)
    } >>
      executeFailureAction(messageId, deliveryTag, properties, routingKey, body)
  }

  private def executeFailureAction(messageId: String,
                                   deliveryTag: Long,
                                   properties: BasicProperties,
                                   routingKey: String,
                                   body: Array[Byte]): F[Unit] = {
    handleResult(messageId, deliveryTag, properties, routingKey, body)(failureAction)
  }
}
