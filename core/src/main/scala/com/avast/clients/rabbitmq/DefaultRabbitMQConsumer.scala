package com.avast.clients.rabbitmq

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Effect, IO, Sync}
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{DefaultConsumer, Envelope, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class DefaultRabbitMQConsumer[F[_]: Effect](
    override val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    override protected val monitor: Monitor,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    override protected val blockingScheduler: Scheduler)(readAction: DeliveryReadAction[F, Bytes])(implicit scheduler: Scheduler)
    extends DefaultConsumer(channel)
    with RabbitMQConsumer[F]
    with ConsumerBase[F]
    with StrictLogging {

  import DefaultRabbitMQConsumer._

  private val readMeter = monitor.meter("read")

  private val processingFailedMeter = resultsMonitor.meter("processingFailed")

  private val tasksMonitor = monitor.named("tasks")

  private val processingCount = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  private val processedTimer = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, name, consumerTag, sig)

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    processingCount.incrementAndGet()

    val deliveryTag = envelope.getDeliveryTag
    val messageId = properties.getMessageId
    val routingKey = Option(properties.getHeaders).flatMap(p => Option(p.get(RepublishOriginalRoutingKeyHeaderName))) match {
      case Some(originalRoutingKey) => originalRoutingKey.toString
      case None => envelope.getRoutingKey
    }

    handleDelivery(messageId, deliveryTag, properties, routingKey, body)
      .andThen {
        case Success(_) =>
          processingCount.decrementAndGet()
          logger.debug(s"Delivery processed successfully (tag $deliveryTag)")

        case Failure(NonFatal(e)) =>
          processingCount.decrementAndGet()
          processingFailedMeter.mark()
          logger.debug("Could not process delivery", e)
      }

    ()
  }

  private def handleDelivery(messageId: String,
                             deliveryTag: Long,
                             properties: BasicProperties,
                             routingKey: String,
                             body: Array[Byte]): Future[Unit] = {
    try {
      readMeter.mark()

      logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

      val delivery = Delivery(Bytes.copyFrom(body), properties.asScala, Option(routingKey).getOrElse(""))

      logger.trace(s"[$name] Received delivery: $delivery")

      processedTimer
        .time {
          toIO {
            readAction(delivery)
              .flatMap {
                handleResult(messageId, deliveryTag, properties, routingKey, body)
              }
          }.unsafeToFuture()
        }
        .recoverWith {
          case NonFatal(t) => handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)(t)
        }
    } catch {
      // we catch this specific exception, handling of others is up to Lyra
      case e: RejectedExecutionException =>
        logger.debug(s"[$name] Executor was unable to plan the handling task", e)
        handleFailure(messageId, deliveryTag, properties, routingKey, body, e)

      case NonFatal(e) => handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)(e)
    }
  }

  private def handleCallbackFailure(messageId: String,
                                    deliveryTag: Long,
                                    properties: BasicProperties,
                                    routingKey: String,
                                    body: Array[Byte])(t: Throwable): Future[Unit] = {

    logger.error(s"[$name] Error while executing callback, it's probably a BUG", t)

    handleFailure(messageId, deliveryTag, properties, routingKey, body, t)
  }

  private def handleFailure(messageId: String,
                            deliveryTag: Long,
                            properties: BasicProperties,
                            routingKey: String,
                            body: Array[Byte],
                            t: Throwable): Future[Unit] = {
    processingCount.decrementAndGet()
    processingFailedMeter.mark()
    consumerListener.onError(this, name, channel, t)

    toIO {
      executeFailureAction(messageId, deliveryTag, properties, routingKey, body)
    }.unsafeToFuture()
  }

  private def executeFailureAction(messageId: String,
                                   deliveryTag: Long,
                                   properties: BasicProperties,
                                   routingKey: String,
                                   body: Array[Byte]): F[Unit] = {
    handleResult(messageId, deliveryTag, properties, routingKey, body)(failureAction)
  }

  override def close(): F[Unit] = Sync[F].delay {
    channel.close()
  }

  private def toIO[A](f: F[A]): IO[A] =
    IO.async { cb =>
      Effect[F].runAsync(f)(r => IO(cb(r))).unsafeRunSync()
    }
}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key"
}
