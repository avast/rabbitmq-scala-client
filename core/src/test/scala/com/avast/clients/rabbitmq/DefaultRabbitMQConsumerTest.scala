package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.clients.rabbitmq.RabbitMQFactory.DefaultListeners
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Future.successful(DeliveryResult.Ack)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should RETRY") {
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Future.successful(DeliveryResult.Retry)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should REJECT") {
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Future.successful(DeliveryResult.Reject)
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(1)).basicReject(deliveryTag, false)
      verify(channel, times(0)).basicPublish("", "queueName", properties, body)
    }
  }

  test("should REPUBLISH") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]

    val consumer = new DefaultRabbitMQConsumer(
      "test",
      channel,
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Reject,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Future.successful(DeliveryResult.Republish())
    })

    val body = Random.nextString(5).getBytes
    consumer.handleDelivery("abcd", envelope, properties, body)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(1)).basicPublish(Matchers.eq(""), Matchers.eq("queueName"), any(), Matchers.eq(body))
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Retry,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      Future.failed(new RuntimeException)
    })

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Retry,
      DefaultListeners.DefaultConsumerListener,
      (_, _) => ???
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      throw new RuntimeException
    })

    consumer.handleDelivery("abcd", envelope, properties, Random.nextString(5).getBytes)

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
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
      "queueName",
      true,
      Monitor.noOp,
      DeliveryResult.Retry,
      DefaultListeners.DefaultConsumerListener,
      (exchange, routingKey) => {
        channel.queueBind(queueName, exchange, routingKey)
      }
    )({ delivery =>
      assertResult(Some(messageId))(delivery.properties.messageId)

      throw new RuntimeException
    })

    consumer.bindTo(exchange, routingKey)

    verify(channel, times(1)).queueBind(queueName, exchange, routingKey)
    ()
  }
}
