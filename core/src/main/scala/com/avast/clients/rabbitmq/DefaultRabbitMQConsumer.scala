package com.avast.clients.rabbitmq

import java.time.{Duration, Instant}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

import cats.effect._
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import JavaConverters._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Delivery => _, _}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQConsumer[F[_]: Effect](
    override val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    override protected val connectionInfo: RabbitMQConnectionInfo,
    override protected val monitor: Monitor,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    override protected val blocker: Blocker)(readAction: DeliveryReadAction[F, Bytes])(implicit override protected val cs: ContextShift[F])
    extends DefaultConsumer(channel)
    with RabbitMQConsumer[F]
    with ConsumerBase[F]
    with StrictLogging {

  import DefaultRabbitMQConsumer._

  override protected implicit val F: Sync[F] = Effect[F]

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

    val action = handleDelivery(messageId, deliveryTag, properties, routingKey, body)
      .flatTap(_ =>
        F.delay {
          processingCount.decrementAndGet()
          logger.debug(s"Delivery processed successfully (tag $deliveryTag)")
      })
      .recoverWith {
        case e =>
          F.delay {
            processingCount.decrementAndGet()
            processingFailedMeter.mark()
            logger.debug("Could not process delivery", e)
          } >>
            F.raiseError(e)
      }

    toIO(action).unsafeToFuture() // actually start the processing

    ()
  }

  private def handleDelivery(messageId: String,
                             deliveryTag: Long,
                             properties: BasicProperties,
                             routingKey: String,
                             body: Array[Byte]): F[Unit] =
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
          .flatMap {
            handleResult(messageId, deliveryTag, properties, routingKey, body)
          }
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

  private def toIO[A](f: F[A]): IO[A] =
    IO.async { cb =>
      Effect[F].runAsync(f)(r => IO(cb(r))).unsafeRunSync()
    }
}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key"
  final val RepublishOriginalUserId = "X-Original-User-Id"
}
