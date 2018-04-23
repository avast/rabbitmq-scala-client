package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.RepublishOriginalRoutingKeyHeaderName
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Reject, Republish, Retry}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[rabbitmq] trait ConsumerBase extends StrictLogging {
  protected def name: String
  protected def queueName: String
  protected def channel: ServerChannel
  protected def blockingScheduler: Scheduler
  protected def monitor: Monitor

  protected val resultsMonitor: Monitor = monitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")

  protected def handleResult(messageId: String, deliveryTag: Long, properties: BasicProperties, routingKey: String, body: Array[Byte])(
      res: DeliveryResult): Task[Unit] = {
    import DeliveryResult._

    res match {
      case Ack => ack(messageId, deliveryTag)
      case Reject => reject(messageId, deliveryTag)
      case Retry => retry(messageId, deliveryTag)
      case Republish(newHeaders) =>
        republish(messageId, deliveryTag, mergeHeadersForRepublish(newHeaders, properties, routingKey), body)
    }
  }

  protected def ack(messageId: String, deliveryTag: Long): Task[Unit] =
    Task {
      try {
        logger.debug(s"[$name] ACK delivery ID $messageId, deliveryTag $deliveryTag")
        channel.basicAck(deliveryTag, false)
        resultAckMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
      }
    }.executeOn(blockingScheduler)

  protected def reject(messageId: String, deliveryTag: Long): Task[Unit] =
    Task {
      try {
        logger.debug(s"[$name] REJECT delivery ID $messageId, deliveryTag $deliveryTag")
        channel.basicReject(deliveryTag, false)
        resultRejectMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
      }
    }.executeOn(blockingScheduler)

  protected def retry(messageId: String, deliveryTag: Long): Task[Unit] =
    Task {
      try {
        logger.debug(s"[$name] REJECT (with requeue) delivery ID $messageId, deliveryTag $deliveryTag")
        channel.basicReject(deliveryTag, true)
        resultRetryMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
      }
    }.executeOn(blockingScheduler)

  protected def republish(messageId: String, deliveryTag: Long, properties: BasicProperties, body: Array[Byte]): Task[Unit] =
    Task {
      try {
        logger.debug(s"[$name] Republishing delivery (ID $messageId, deliveryTag $deliveryTag) to end of queue '$queueName'")
        channel.basicPublish("", queueName, properties, body)
        channel.basicAck(deliveryTag, false)
        resultRepublishMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while republishing the delivery", e)
      }
    }.executeOn(blockingScheduler)

  protected def mergeHeadersForRepublish(newHeaders: Map[String, AnyRef],
                                         properties: BasicProperties,
                                         routingKey: String): BasicProperties = {
    // values in newHeaders will overwrite values in original headers
    val h = newHeaders + (RepublishOriginalRoutingKeyHeaderName -> routingKey)
    val headers = Option(properties.getHeaders).map(_.asScala ++ h).getOrElse(h)
    properties.builder().headers(headers.asJava).build()
  }
}
