package com.avast.clients.rabbitmq

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.Effect
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.{AMQP, GetResponse}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.language.higherKinds
import scala.util.control.NonFatal
class DefaultRabbitMQPullConsumer[F[_]: Effect, A: DeliveryConverter](
    override val name: String,
    protected override val channel: ServerChannel,
    protected override val queueName: String,
    failureAction: DeliveryResult,
    protected override val monitor: Monitor,
    protected override val blockingScheduler: Scheduler)(implicit sch: Scheduler)
    extends RabbitMQPullConsumer[F, A]
    with ConsumerBase[F]
    with AutoCloseable
    with StrictLogging {

  private val tasksMonitor = monitor.named("tasks")

  private val processingCount = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  private def convertMessage(b: Bytes): Either[ConversionException, A] = implicitly[DeliveryConverter[A]].convert(b)

  override def pull(): F[PullResult[F, A]] = {
    Task {
      Option(channel.basicGet(queueName, false))
    }.executeOn(blockingScheduler, forceAsync = true) // blocking operation!
      .asyncBoundary
      .to[F]
      .flatMap {
        case Some(response) =>
          processingCount.incrementAndGet()

          val envelope = response.getEnvelope
          val properties = response.getProps

          val deliveryTag = envelope.getDeliveryTag
          val messageId = properties.getMessageId
          val routingKey = envelope.getRoutingKey

          logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

          handleMessage(response, properties, routingKey) { result =>
            super
              .handleResult(messageId, deliveryTag, properties, routingKey, response.getBody)(result)
              .map { _ =>
                processingCount.decrementAndGet()
                ()
              }
          }

        case None =>
          Effect[F].pure {
            PullResult.EmptyQueue.asInstanceOf[PullResult[F, A]]
          }
      }
  }

  private def handleMessage(response: GetResponse, properties: AMQP.BasicProperties, routingKey: String)(
      handleResult: DeliveryResult => F[Unit]): F[PullResult[F, A]] = {
    try {
      val bytes = Bytes.copyFrom(response.getBody)

      val delivery = convertMessage(bytes) match {
        case Right(a) =>
          val delivery = Delivery(a, properties.asScala, routingKey)
          logger.trace(s"[$name] Received delivery: ${delivery.copy(body = bytes)}")
          delivery

        case Left(ce) =>
          val delivery = Delivery.MalformedContent(bytes, properties.asScala, routingKey, ce)
          logger.trace(s"[$name] Received delivery but could not convert it: $delivery")
          delivery
      }

      val dwh = createDeliveryWithHandle(delivery, handleResult)

      Effect[F].pure {
        PullResult.Ok(dwh)
      }
    } catch {
      case NonFatal(e) =>
        logger.error(
          s"[$name] Error while converting the message, it's probably a BUG; the converter should return Left(ConversionException)",
          e
        )

        handleResult(failureAction).flatMap(_ => Effect[F].raiseError(e))
    }
  }

  private def createDeliveryWithHandle[B](d: Delivery[B], handleResult: DeliveryResult => F[Unit]): DeliveryWithHandle[F, B] = {
    new DeliveryWithHandle[F, B] {
      override val delivery: Delivery[B] = d

      override def handle(result: DeliveryResult): F[Unit] = handleResult(result)
    }
  }

  override def close(): Unit = {
    channel.close()
  }
}
