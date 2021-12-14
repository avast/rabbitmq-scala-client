package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.time.{Seconds, Span}
import org.slf4j.event.Level

import java.util.UUID
import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Random

class RepublishStrategyTest extends TestBase {

  private val connectionInfo = RabbitMQConnectionInfo(immutable.Seq("localhost"), "/", None)

  test("default exchange") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val originalUserId = "OriginalUserId"
    val properties = new BasicProperties.Builder().messageId(messageId).userId(originalUserId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, RepublishStrategy.DefaultExchange[Task]()) { delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Republish())
    }

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)

      val propertiesCaptor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel, times(1)).basicPublish(Matchers.eq(""), Matchers.eq("queueName"), propertiesCaptor.capture(), Matchers.eq(body))
      assertResult(Some(originalUserId))(propertiesCaptor.getValue.getHeaders.asScala.get(DefaultRabbitMQConsumer.RepublishOriginalUserId))
    }
  }

  test("custom exchange") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val originalUserId = "OriginalUserId"
    val properties = new BasicProperties.Builder().messageId(messageId).userId(originalUserId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val consumer = newConsumer(channel, RepublishStrategy.CustomExchange("myCustomExchange")) { delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Task.now(DeliveryResult.Republish())
    }

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)

      val propertiesCaptor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel, times(1)).basicPublish(Matchers.eq("myCustomExchange"),
                                             Matchers.eq("queueName"),
                                             propertiesCaptor.capture(),
                                             Matchers.eq(body))
      assertResult(Some(originalUserId))(propertiesCaptor.getValue.getHeaders.asScala.get(DefaultRabbitMQConsumer.RepublishOriginalUserId))
    }
  }

  private def newConsumer(channel: ServerChannel, republishStrategy: RepublishStrategy[Task])(
      userAction: DeliveryReadAction[Task, Bytes]): DefaultRabbitMQConsumer[Task, Bytes] = {
    val base = new ConsumerBase[Task, Bytes]("test", "queueName", TestBase.testBlocker, ImplicitContextLogger.createLogger, Monitor.noOp())

    val channelOps = new ConsumerChannelOps[Task, Bytes](
      "test",
      "queueName",
      channel,
      TestBase.testBlocker,
      republishStrategy,
      PMH,
      connectionInfo,
      ImplicitContextLogger.createLogger,
      Monitor.noOp()
    )

    new DefaultRabbitMQConsumer[Task, Bytes](
      base,
      channelOps,
      10.seconds,
      DeliveryResult.Republish(),
      Level.ERROR,
      Republish(),
      DefaultListeners.defaultConsumerListener,
    )(userAction)
  }

  object PMH extends LoggingPoisonedMessageHandler[Task, Bytes](3)
}
