package com.avast.clients.rabbitmq

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.kluzo.{Kluzo, TraceId}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{DefaultConsumer, Envelope, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Callback, Task}
import monix.execution.Scheduler

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class DefaultRabbitMQConsumer(
    override val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    useKluzo: Boolean,
    override protected val monitor: Monitor,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    override protected val blockingScheduler: Scheduler)(readAction: DeliveryReadAction[Task, Bytes])(implicit callbackScheduler: Scheduler)
    extends DefaultConsumer(channel)
    with RabbitMQConsumer
    with ConsumerBase
    with AutoCloseable
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

    val traceId = extractTraceId(properties)

    Kluzo.withTraceId(traceId) {
      logger.debug(s"[$name] Kluzo Id: $traceId")

      val deliveryTag = envelope.getDeliveryTag
      val messageId = properties.getMessageId
      val routingKey = Option(properties.getHeaders).flatMap(p => Option(p.get(RepublishOriginalRoutingKeyHeaderName))) match {
        case Some(originalRoutingKey) => originalRoutingKey.toString
        case None => envelope.getRoutingKey
      }

      val task = handleDelivery(messageId, deliveryTag, properties, routingKey, body)

      processedTimer.time {
        task.runAsync(new Callback[Unit] {
          override def onSuccess(value: Unit): Unit = processingCount.decrementAndGet()

          override def onError(ex: Throwable): Unit = processingCount.decrementAndGet()
        })
      }

      ()
    }
  }

  private def handleDelivery(messageId: String,
                             deliveryTag: Long,
                             properties: BasicProperties,
                             routingKey: String,
                             body: Array[Byte]): Task[Unit] = {
    {
      try {
        readMeter.mark()

        logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

        val delivery = Delivery(Bytes.copyFrom(body), properties.asScala, Option(routingKey).getOrElse(""))

        logger.trace(s"[$name] Received delivery: $delivery")

        readAction(delivery)
          .flatMap {
            handleResult(messageId, deliveryTag, properties, routingKey, body)
          }
          .onErrorHandleWith {
            handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)
          }
          .executeOn(callbackScheduler)
      } catch {
        // we catch this specific exception, handling of others is up to Lyra
        case e: RejectedExecutionException =>
          logger.debug(s"[$name] Executor was unable to plan the handling task", e)
          handleFailure(messageId, deliveryTag, properties, routingKey, body, e)

        case NonFatal(e) => handleCallbackFailure(messageId, deliveryTag, properties, routingKey, body)(e)
      }
    }.executeOn(callbackScheduler)
  }

  private def extractTraceId(properties: BasicProperties) = {
    if (useKluzo) {
      val traceId = Option(properties.getHeaders)
        .flatMap(h => Option(h.get(Kluzo.HttpHeaderName)))
        .map(_.toString)
        .map(TraceId(_))
        .getOrElse(TraceId.generate)

      Some(traceId)
    } else {
      None
    }
  }

  private def handleCallbackFailure(messageId: String,
                                    deliveryTag: Long,
                                    properties: BasicProperties,
                                    routingKey: String,
                                    body: Array[Byte])(t: Throwable): Task[Unit] = {

    logger.error(s"[$name] Error while executing callback, it's probably a BUG", t)

    handleFailure(messageId, deliveryTag, properties, routingKey, body, t)
  }

  private def handleFailure(messageId: String,
                            deliveryTag: Long,
                            properties: BasicProperties,
                            routingKey: String,
                            body: Array[Byte],
                            t: Throwable): Task[Unit] = {
    processingCount.decrementAndGet()
    processingFailedMeter.mark()
    consumerListener.onError(this, name, channel, t)
    executeFailureAction(messageId, deliveryTag, properties, routingKey, body)
  }

  private def executeFailureAction(messageId: String,
                                   deliveryTag: Long,
                                   properties: BasicProperties,
                                   routingKey: String,
                                   body: Array[Byte]): Task[Unit] = {
    import DeliveryResult._

    failureAction match {
      case Ack => ack(messageId, deliveryTag)
      case Reject => reject(messageId, deliveryTag)
      case Retry => retry(messageId, deliveryTag)
      case Republish(newHeaders) => republish(messageId, deliveryTag, mergeHeadersForRepublish(newHeaders, properties, routingKey), body)
    }
  }

  override def close(): Unit = {
    channel.close()
  }

}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key"
}
