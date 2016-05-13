package com.avast.clients.rabbitmq

import java.time.{Clock, Duration}

import com.avast.clients.rabbitmq.RabbitMQClientFactory.ServerChannel
import com.avast.clients.rabbitmq.api.{RabbitMQReceiver, RabbitMQSender, Result}
import com.avast.metrics.api.Monitor
import com.avast.utils2.errorhandling.FutureTimeouter._
import com.rabbitmq.client._
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class RabbitMQClient(name: String,
                     queueSettings: QueueSettings,
                     channel: ServerChannel,
                     processTimeout: Duration,
                     clock: Clock,
                     monitor: Monitor,
                     receiveAction: Option[Delivery => Future[Result]])
                    (implicit ec: ExecutionContext) extends RabbitMQSender with RabbitMQReceiver with StrictLogging {

  import queueSettings._

  channel.addRecoveryListener(ChannelRecoveryListener)
  channel.addShutdownListener(ChannelShutdownListener)

  receiveAction.foreach { action =>
    val consumer = new RabbitMQConsumer(name, channel, clock, monitor)({ delivery =>
      import Result._
      try {
        action(delivery)
          .timeoutAfter(processTimeout)
          .andThen {
            case Success(Ok) => // ok
            case Success(Failed) =>
              requeue(delivery)
            case Failure(NonFatal(e)) =>
              logger.warn("Error while executing callback, automatically requeuing", e)
              requeue(delivery)
          }
      } catch {
        case NonFatal(e) =>
          logger.error("Error while executing callback, automatically requeuing", e)
          requeue(delivery)
      }
    })

    channel.basicConsume(queueName, false, consumer)
  }

  protected def requeue(delivery: Delivery): Unit = {
    import delivery._
    send(body, routingKey, properties)
  }

  override def send(body: Array[Byte], routingKey: String, properties: AMQP.BasicProperties): Unit = {
    channel.basicPublish(exchange, routingKey, properties, body)

    ???
  }

  override def send(body: Array[Byte], properties: AMQP.BasicProperties): Unit = send(body, null, properties)

  override def send(body: Array[Byte], routingKey: String): Unit = send(body, routingKey, null)

  override def send(body: Array[Byte]): Unit = send(body, null, null)

  override def close(): Unit = {
    channel.close()
  }

  private object ChannelShutdownListener extends ShutdownListener with LazyLogging {
    override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
      if (!cause.isInitiatedByApplication) {
        logger.info(s"Channel of $name was closed, waiting for recover", cause)
      }
      else {
        logger.debug(s"Channel of $name was closed")
      }
    }
  }

  private object ChannelRecoveryListener extends RecoveryListener with LazyLogging {
    override def handleRecovery(recoverable: Recoverable): Unit = {
      logger.info(s"Channel of $name was recovered")
    }
  }

}


case class QueueSettings(exchange: String, queueName: String)

case class Delivery(body: Array[Byte], properties: AMQP.BasicProperties, routingKey: String)
