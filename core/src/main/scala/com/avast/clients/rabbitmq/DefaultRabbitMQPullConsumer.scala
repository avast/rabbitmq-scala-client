package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.atomic.AtomicInteger

class DefaultRabbitMQPullConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
    override val name: String,
    protected override val channel: ServerChannel,
    protected override val queueName: String,
    protected override val connectionInfo: RabbitMQConnectionInfo,
    override protected val republishStrategy: RepublishStrategy,
    override protected val poisonedMessageHandler: PoisonedMessageHandler[F, A],
    protected override val monitor: Monitor,
    protected override val blocker: Blocker)(implicit override protected val cs: ContextShift[F])
    extends RabbitMQPullConsumer[F, A]
    with ConsumerBase[F, A]
    with StrictLogging {

  override protected val deliveryConverter: DeliveryConverter[A] = implicitly

  override protected implicit val F: Sync[F] = Sync[F]

  private val tasksMonitor = monitor.named("tasks")

  private val processingCount = new AtomicInteger(0)
  tasksMonitor.gauge("processing")(() => processingCount.get())

  override def pull(): F[PullResult[F, A]] = {
    blocker
      .delay {
        Option(channel.basicGet(queueName, false))
      }
      .flatMap {
        case Some(response) =>
          processingCount.incrementAndGet()

          parseDelivery(response.getEnvelope, response.getBody, response.getProps).flatMap { d =>
            import d._
            import metadata._

            logger.debug(s"[$name] Read delivery with $messageId/$correlationId $deliveryTag")

            val dwh = createDeliveryWithHandle(delivery) { result =>
              super
                .handleResult(messageId, correlationId, deliveryTag, fixedProperties, routingKey, response.getBody, delivery)(result)
                .map { _ =>
                  processingCount.decrementAndGet()
                  ()
                }
            }

            Effect[F].pure {
              PullResult.Ok(dwh)
            }
          }

        case None =>
          Effect[F].pure {
            PullResult.EmptyQueue.asInstanceOf[PullResult[F, A]]
          }
      }
  }

  private def createDeliveryWithHandle[B](d: Delivery[B])(handleResult: DeliveryResult => F[Unit]): DeliveryWithHandle[F, B] = {
    new DeliveryWithHandle[F, B] {
      override val delivery: Delivery[B] = d

      override def handle(result: DeliveryResult): F[Unit] = handleResult(result)
    }
  }
}
