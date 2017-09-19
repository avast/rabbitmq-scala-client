package com.avast.clients.rabbitmq

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQFactory.ServerChannel
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.kluzo.{Kluzo, TraceId}
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.AMQP.Queue.BindOk
import com.rabbitmq.client.{DefaultConsumer, Envelope, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DefaultRabbitMQConsumer(
    name: String,
    channel: ServerChannel,
    queueName: String,
    useKluzo: Boolean,
    monitor: Monitor,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    bindToAction: (String, String) => BindOk)(readAction: Delivery => Future[DeliveryResult])(implicit ec: ExecutionContext)
    extends DefaultConsumer(channel)
    with RabbitMQConsumer
    with StrictLogging {

  private val readMeter = monitor.meter("read")
  private val resultsMonitor = monitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")
  private val processingFailedMeter = resultsMonitor.meter("processingFailed")

  private val tasksMonitor = monitor.named("tasks")

  private val processingCount = new AtomicInteger(0)

  tasksMonitor.gauge("processing")(() => processingCount.get())

  private val processedTimer = tasksMonitor.timerPair("processed")

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, consumerTag, sig)

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    processingCount.incrementAndGet()

    val traceId = extractTraceId(properties)

    val deliveryTag = envelope.getDeliveryTag
    val messageId = properties.getMessageId

    Kluzo.withTraceId(traceId) {
      try {
        ec.execute(() => {
          try {
            readMeter.mark()

            logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

            val message = Delivery(Bytes.copyFrom(body), properties.asScala, Option(envelope.getRoutingKey).getOrElse(""))

            processedTimer.time {
              readAction(message).andThen(handleResult(messageId, deliveryTag, properties, body))
            }

            ()
          } catch {
            case NonFatal(e) =>
              processingCount.decrementAndGet()
              processingFailedMeter.mark()
              logger.error(s"[$name] Error while executing callback, it's probably u BUG", e)
              consumerListener.onError(this, channel, e)
              executeFailureAction(messageId, deliveryTag, properties, body)
          }
        })
      } catch {
        // we catch this specific exception, handling of others is up to Lyra
        case e: RejectedExecutionException =>
          processingCount.decrementAndGet()
          processingFailedMeter.mark()
          logger.error(s"[$name] Executor was unable to plan the handling task", e)
          consumerListener.onError(this, channel, e)
          executeFailureAction(messageId, deliveryTag, properties, body)
      }
    }
  }

  private def extractTraceId(properties: BasicProperties) = {
    if (useKluzo && properties.getHeaders != null) {
      val traceId = Option(properties.getHeaders.get(Kluzo.HttpHeaderName))
        .map(_.toString)
        .map(TraceId(_))
        .getOrElse(TraceId.generate)

      Some(traceId)
    } else {
      None
    }
  }

  private def handleResult(messageId: String,
                           deliveryTag: Long,
                           properties: BasicProperties,
                           body: Array[Byte]): PartialFunction[Try[DeliveryResult], Unit] = {
    import DeliveryResult._

    processingCount.decrementAndGet()

    {
      case Success(Ack) => ack(messageId, deliveryTag)
      case Success(Reject) => reject(messageId, deliveryTag)
      case Success(Retry) => retry(messageId, deliveryTag)
      case Success(Republish(newHeaders)) => republish(messageId, deliveryTag, mergeHeaders(newHeaders, properties), body)
      case Failure(NonFatal(e)) =>
        processingFailedMeter.mark()
        logger.error(s"[$name] Error while executing callback, it's probably a BUG", e)

        executeFailureAction(messageId, deliveryTag, properties, body)
    }
  }

  private def executeFailureAction(messageId: String, deliveryTag: Long, properties: BasicProperties, body: Array[Byte]): Unit = {
    import DeliveryResult._

    failureAction match {
      case Ack => ack(messageId, deliveryTag)
      case Reject => reject(messageId, deliveryTag)
      case Retry => retry(messageId, deliveryTag)
      case Republish(newHeaders) => republish(messageId, deliveryTag, mergeHeaders(newHeaders, properties), body)
    }
  }

  private def mergeHeaders(newHeaders: Map[String, AnyRef], properties: BasicProperties): BasicProperties = {
    if (newHeaders.isEmpty) properties
    else {
      // values in newHeaders will overwrite values in original headers
      val headers = Option(properties.getHeaders).map(_.asScala ++ newHeaders).getOrElse(newHeaders)

      properties.builder().headers(headers.asJava).build()
    }
  }

  override def bindTo(exchange: String, routingKey: String): Try[Done] = Try {
    bindToAction(exchange, routingKey)
    Done
  }

  override def close(): Unit = {
    channel.close()
  }

  private def ack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] ACK delivery ID $messageId, deliveryTag $deliveryTag")
      channel.basicAck(deliveryTag, false)
      resultAckMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
    }
  }

  private def reject(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT delivery ID $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, false)
      resultRejectMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
    }
  }

  private def retry(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT (with requeue) delivery ID $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, true)
      resultRetryMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
    }
  }

  private def republish(messageId: String, deliveryTag: Long, properties: BasicProperties, body: Array[Byte]): Unit = {
    try {
      logger.debug(s"[$name] Republishing delivery (ID $messageId, deliveryTag $deliveryTag) to end of queue '$queueName'")
      channel.basicPublish("", queueName, properties, body)
      channel.basicAck(deliveryTag, false)
      resultRepublishMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while republishing the delivery", e)
    }
  }
}
