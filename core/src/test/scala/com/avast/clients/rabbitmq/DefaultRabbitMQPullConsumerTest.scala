package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ConversionException, DeliveryResult, PullResult}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import com.rabbitmq.client.{Envelope, GetResponse}
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future
import scala.util.Random

class DefaultRabbitMQPullConsumerTest extends FunSuite with MockitoSugar with ScalaFutures with Eventually {
  test("should ACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Future, Bytes](
      "test",
      channel,
      "queueName",
      DeliveryResult.Reject,
      (_, _, _) => fail(),
      Monitor.noOp,
      Scheduler.global
    )

    val PullResult.Ok(dwh) = consumer.pull().futureValue

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Ack).futureValue

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

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Future, Bytes](
      "test",
      channel,
      "queueName",
      DeliveryResult.Reject,
      (_, _, _) => fail(),
      Monitor.noOp,
      Scheduler.global
    )

    val PullResult.Ok(dwh) = consumer.pull().futureValue

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Retry).futureValue

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

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Future, Bytes](
      "test",
      channel,
      "queueName",
      DeliveryResult.Ack,
      (_, _, _) => fail(),
      Monitor.noOp,
      Scheduler.global
    )

    val PullResult.Ok(dwh) = consumer.pull().futureValue

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Reject).futureValue

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

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Future, Bytes](
      "test",
      channel,
      "queueName",
      DeliveryResult.Reject,
      (_, _, _) => fail(),
      Monitor.noOp,
      Scheduler.global
    )

    val PullResult.Ok(dwh) = consumer.pull().futureValue

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Republish()).futureValue

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      verify(channel, times(1)).basicPublish(Matchers.eq(""), Matchers.eq("queueName"), any(), Matchers.eq(body))
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

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    case class Abc(i: Int)

    implicit val c: DeliveryConverter[Abc] = (_: Bytes) => {
      throw new IllegalArgumentException
    }

    val consumer = new DefaultRabbitMQPullConsumer[Future, Abc](
      "test",
      channel,
      "queueName",
      DeliveryResult.Retry,
      (_, _, _) => fail(),
      Monitor.noOp,
      Scheduler.global
    )

    try {
      consumer.pull().futureValue
    } catch {
      case e: TestFailedException if e.getCause.isInstanceOf[IllegalArgumentException] => // ok
    }

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
    }
  }

  test("should NACK because of conversion failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = mock[BasicProperties]
    when(properties.getMessageId).thenReturn(messageId)

    val channel = mock[AutorecoveringChannel]

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    case class Abc(i: Int)

    implicit val c: DeliveryConverter[Abc] = (_: Bytes) => {
      Left(ConversionException(""))
    }

    val consumer = new DefaultRabbitMQPullConsumer[Future, Abc](
      "test",
      channel,
      "queueName",
      DeliveryResult.Ack,
      (_, _, _) => Future.successful(DeliveryResult.Retry),
      Monitor.noOp,
      Scheduler.global
    )

    try {
      consumer.pull().futureValue
    } catch {
      case e: TestFailedException if e.getCause.isInstanceOf[ConversionException] => // ok
    }

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
    }
  }
}
