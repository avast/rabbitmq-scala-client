package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties, RabbitMQProducer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.kluzo.Kluzo
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ReturnListener
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_]: FromTask, A: ProductConverter](name: String,
                                                                   exchangeName: String,
                                                                   channel: ServerChannel,
                                                                   defaultProperties: MessageProperties,
                                                                   useKluzo: Boolean,
                                                                   reportUnroutable: Boolean,
                                                                   scheduler: Scheduler,
                                                                   monitor: Monitor)
    extends RabbitMQProducer[F, A]
    with AutoCloseable
    with StrictLogging {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)

  override def send(routingKey: String, body: A, properties: Option[MessageProperties] = None): F[Unit] = {
    val finalProperties = {
      val initialProperties = properties.getOrElse(defaultProperties.copy(messageId = Some(UUID.randomUUID().toString)))
      val messageProperties = converter.fillProperties(initialProperties)

      if (useKluzo && Kluzo.getTraceId.nonEmpty) {

        val headers = {
          val headers = messageProperties.headers

          headers
            .get(Kluzo.HttpHeaderName)
            .map(_.toString)
            .orElse(Kluzo.getTraceId.map(_.value)) match {
            case Some(traceId) => headers + (Kluzo.HttpHeaderName -> traceId)
            case None => headers
          }
        }

        messageProperties.copy(headers = headers)
      } else {
        messageProperties
      }
    }

    val task = converter.convert(body) match {
      case Right(convertedBody) => send(routingKey, convertedBody, finalProperties)
      case Left(ce) => Task.raiseError(ce)
    }

    implicitly[FromTask[F]].apply(task)
  }

  private def send(routingKey: String, body: Bytes, properties: MessageProperties): Task[Unit] = {
    Task {
      try {
        sendLock.synchronized {
          // see https://www.rabbitmq.com/api-guide.html#channel-threads
          channel.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
        }

        // ok!
        sentMeter.mark()
      } catch {
        case NonFatal(e) =>
          logger.debug(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'", e)
          sentFailedMeter.mark()
          throw e
      }
    }.executeOn(scheduler)
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
