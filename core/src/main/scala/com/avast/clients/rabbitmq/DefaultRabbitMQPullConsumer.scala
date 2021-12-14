package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._

import java.util.concurrent.atomic.AtomicInteger

class DefaultRabbitMQPullConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](base: ConsumerBase[F, A],
                                                                                channelOps: ConsumerChannelOps[F, A])
    extends RabbitMQPullConsumer[F, A] {
  import base._
  import channelOps._

  private val tasksMonitor = consumerRootMonitor.named("tasks")

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

          val rawBody = Bytes.copyFrom(response.getBody)

          parseDelivery(response.getEnvelope, rawBody, response.getProps).flatMap { d =>
            import d._
            import metadata._

            val dwh = createDeliveryWithHandle(delivery) { result =>
              handleResult(messageId, deliveryTag, fixedProperties, routingKey, rawBody, delivery)(result)
                .map { _ =>
                  processingCount.decrementAndGet()
                  ()
                }
            }

            consumerLogger.debug(s"[$consumerName] Read delivery with $messageId $deliveryTag").as {
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
