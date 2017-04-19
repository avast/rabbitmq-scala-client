package com.avast.clients.rabbitmq

import java.util
import java.util.UUID

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQProducer
import com.avast.kluzo.Kluzo
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, ReturnListener}
import com.typesafe.scalalogging.StrictLogging

import scala.util.Try

class DefaultRabbitMQProducer(name: String,
                              exchangeName: String,
                              channel: ServerChannel,
                              useKluzo: Boolean,
                              reportUnroutable: Boolean,
                              monitor: Monitor)
    extends RabbitMQProducer
    with StrictLogging {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)

  override def send(routingKey: String, body: Bytes, properties: AMQP.BasicProperties): Try[Done] = {
    val result = Try {
      // Kluzo enabled and ID available?
      val finalProperties = if (useKluzo && Kluzo.getTraceId.nonEmpty) {
        // create mutable copy or brand new map
        val headers: java.util.Map[String, AnyRef] = Option(properties.getHeaders)
          .map { h =>
            val m = new util.HashMap[String, AnyRef]()
            m.putAll(h)
            m
          }
          .getOrElse(new util.HashMap(2))

        // set TraceId if not already set
        Option(headers.get(Kluzo.HttpHeaderName))
          .orElse(Kluzo.getTraceId.map(_.value))
          .map(_.toString)
          .foreach { id =>
            headers.put(Kluzo.HttpHeaderName, id)
          }

        properties
          .builder()
          .headers(headers)
          .build()
      } else {
        properties
      }

      sendLock.synchronized {
        // see https://www.rabbitmq.com/api-guide.html#channel-threads
        channel.basicPublish(exchangeName, routingKey, finalProperties, body.toByteArray)
      }

      sentMeter.mark()
      Done
    }

    result.failed.foreach { e =>
      sentFailedMeter.mark()
    }

    result
  }

  override def send(routingKey: String, body: Bytes): Try[Done] = {
    val properties = new AMQP.BasicProperties.Builder()
      .messageId(UUID.randomUUID().toString)
      .build()

    send(routingKey, body, properties)
  }

  override def close(): Unit = {
    channel.close()
  }

  // scalastyle:off
  private object LoggingReturnListener extends ReturnListener {
    override def handleReturn(replyCode: Int,
                              replyText: String,
                              exchange: String,
                              routingKey: String,
                              properties: BasicProperties,
                              body: Array[Byte]): Unit = {
      unroutableMeter.mark()
      logger.warn(
        s"[$name] Message sent with routingKey '$routingKey' to exchange '$exchange' (message ID '${properties.getMessageId}', body size ${body.length} B) is unroutable ($replyCode: $replyText)"
      )
    }
  }

  private object NoOpReturnListener extends ReturnListener {
    override def handleReturn(replyCode: Int,
                              replyText: String,
                              exchange: String,
                              routingKey: String,
                              properties: BasicProperties,
                              body: Array[Byte]): Unit = {
      unroutableMeter.mark()
    }
  }

}
