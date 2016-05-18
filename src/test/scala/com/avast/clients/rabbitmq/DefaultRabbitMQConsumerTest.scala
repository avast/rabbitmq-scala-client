package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.metrics.test.NoOpMonitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class DefaultRabbitMQConsumerTest extends FunSuite with MockitoSugar with Eventually {
  test("should ACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      NoOpMonitor.INSTANCE
    )({ delivery =>
      assertResult(messageId)(delivery.properties.getMessageId)

      Future.successful(true)
    })((_, _) => ???)

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicNack(deliveryTag, false, true)
    }
  }

  test("should NACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      NoOpMonitor.INSTANCE
    )({ delivery =>
      assertResult(messageId)(delivery.properties.getMessageId)

      Future.successful(false)
    })((_, _) => ???)

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicNack(deliveryTag, false, true)
    }
  }

  test("should NACK because of failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      NoOpMonitor.INSTANCE
    )({ delivery =>
      assertResult(messageId)(delivery.properties.getMessageId)

      Future.failed(new RuntimeException)
    })((_, _) => ???)

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicNack(deliveryTag, false, true)
    }
  }

  test("should NACK because of unexpected failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      NoOpMonitor.INSTANCE
    )({ delivery =>
      assertResult(messageId)(delivery.properties.getMessageId)

      throw new RuntimeException
    })((_, _) => ???)

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicNack(deliveryTag, false, true)
    }
  }

  test("should bind to another exchange") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val queueName = Random.nextString(10)
    val exchange = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      NoOpMonitor.INSTANCE
    )({ delivery =>
      assertResult(messageId)(delivery.properties.getMessageId)

      throw new RuntimeException
    })((exchange, routingKey) => {
      channel.queueBind(queueName, exchange, routingKey)
    })

    consumer.bindTo(exchange,routingKey)

    verify(channel, times(1)).queueBind(queueName,exchange,routingKey)
    ()
  }
}
