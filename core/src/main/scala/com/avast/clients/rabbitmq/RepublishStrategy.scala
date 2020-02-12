package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ContextShift, Sync}
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.control.NonFatal

trait RepublishStrategy {
  def republish[F[_]: Sync: ContextShift](blocker: Blocker, channel: ServerChannel, consumerName: String)(originalQueueName: String,
                                                                                                          messageId: String,
                                                                                                          deliveryTag: Long,
                                                                                                          properties: BasicProperties,
                                                                                                          body: Array[Byte]): F[Unit]
}

object RepublishStrategy {

  case class CustomExchange(exchangeName: String) extends RepublishStrategy with StrictLogging {
    def republish[F[_]: Sync: ContextShift](blocker: Blocker, channel: ServerChannel, consumerName: String)(originalQueueName: String,
                                                                                                            messageId: String,
                                                                                                            deliveryTag: Long,
                                                                                                            properties: BasicProperties,
                                                                                                            body: Array[Byte]): F[Unit] = {
      blocker.delay {
        try {
          logger.debug {
            s"[$consumerName] Republishing delivery (ID $messageId, deliveryTag $deliveryTag) to end of queue '$originalQueueName' through '$exchangeName'($originalQueueName)"
          }
          if (!channel.isOpen) throw new IllegalStateException("Cannot republish delivery on closed channel")
          channel.basicPublish(exchangeName, originalQueueName, properties, body)
          channel.basicAck(deliveryTag, false)
        } catch {
          case NonFatal(e) => logger.warn(s"[$consumerName] Error while republishing the delivery", e)
        }
      }
    }
  }

  case object DefaultExchange extends RepublishStrategy with StrictLogging {
    def republish[F[_]: Sync: ContextShift](blocker: Blocker, channel: ServerChannel, consumerName: String)(originalQueueName: String,
                                                                                                            messageId: String,
                                                                                                            deliveryTag: Long,
                                                                                                            properties: BasicProperties,
                                                                                                            body: Array[Byte]): F[Unit] = {
      blocker.delay {
        try {
          logger.debug {
            s"[$consumerName] Republishing delivery (ID $messageId, deliveryTag $deliveryTag) to end of queue '$originalQueueName' (through default exchange)"
          }
          if (!channel.isOpen) throw new IllegalStateException("Cannot republish delivery on closed channel")
          channel.basicPublish("", originalQueueName, properties, body)
          channel.basicAck(deliveryTag, false)
        } catch {
          case NonFatal(e) => logger.warn(s"[$consumerName] Error while republishing the delivery", e)
        }
      }
    }
  }
}
