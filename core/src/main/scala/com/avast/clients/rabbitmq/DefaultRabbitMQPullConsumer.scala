package com.avast.clients.rabbitmq

import java.util.concurrent.atomic.AtomicInteger

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

class DefaultRabbitMQPullConsumer[F[_]: FromTask: ToTask, A: DeliveryConverter](
    override val name: String,
    protected override val channel: ServerChannel,
    protected override val queueName: String,
    failureAction: DeliveryResult,
    parsingFailureAction: ParsingFailureAction[F],
    protected override val monitor: Monitor,
    protected override val blockingScheduler: Scheduler)(implicit callbackScheduler: Scheduler)
    extends RabbitMQPullConsumer[F, A]
    with ConsumerBase
    with AutoCloseable
    with StrictLogging {

  private val tasksMonitor = monitor.named("tasks")

  private val processingCount = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  private def convertMessage(b: Bytes): Either[ConversionException, A] = implicitly[DeliveryConverter[A]].convert(b)

  override def pull(): F[PullResult[F, A]] = implicitly[FromTask[F]].apply {
    Task {
      Option(channel.basicGet(queueName, false))
    }.executeOn(blockingScheduler) // blocking operation!
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
              .foreachL { _ =>
                processingCount.decrementAndGet()
                ()
              }
          }

        case None =>
          Task.now {
            PullResult.EmptyQueue.asInstanceOf[PullResult[F, A]]
          }
      }
  }

  private def handleMessage(response: GetResponse, properties: AMQP.BasicProperties, routingKey: String)(
      handleResult: DeliveryResult => Task[Unit]): Task[PullResult[F, A]] = {
    try {
      val bytes = Bytes.copyFrom(response.getBody)

      convertMessage(bytes) match {
        case Right(a) =>
          val d = Delivery(a, properties.asScala, routingKey)
          logger.trace(s"[$name] Received delivery: ${d.copy(body = bytes)}")

          val dwh = createDeliveryWithHandle(d, handleResult)

          Task.now(PullResult.Ok(dwh))

        case Left(ce) =>
          val rawDelivery = Delivery(bytes, properties.asScala, routingKey)

          implicitly[ToTask[F]]
            .apply(parsingFailureAction(name, rawDelivery, ce))
            .flatMap(handleResult)
            .map(_ => PullResult.MalformedContent(rawDelivery, ce))
      }
    } catch {
      case NonFatal(e) =>
        logger.error(
          s"[$name] Error while converting the message, it's probably a BUG; the converter should return Left(ConversionException)",
          e
        )

        handleResult(failureAction).flatMap(_ => Task.raiseError(e))
    }
  }

  private def createDeliveryWithHandle(d: Delivery[A], handleResult: DeliveryResult => Task[Unit]): DeliveryWithHandle[F, A] = {
    new DeliveryWithHandle[F, A] {
      override val delivery: Delivery[A] = d

      override def handle(result: DeliveryResult): F[Unit] = implicitly[FromTask[F]].apply {
        handleResult(result)
      }
    }
  }

  override def close(): Unit = {
    channel.close()
  }
}
