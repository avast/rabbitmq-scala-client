package com.avast.clients.rabbitmq

import java.util.UUID
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.CorrelationIdHeaderName
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import com.rabbitmq.client.{Envelope, GetResponse}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.time.{Seconds, Span}

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.util.Random

class DefaultRabbitMQPullConsumerTest extends TestBase {

  private val connectionInfo = RabbitMQConnectionInfo(immutable.Seq("localhost"), "/", None)

  test("should ACK") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Reject,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Ack).await

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

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Reject,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Retry).await

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

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Ack,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Reject).await

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

    val originalUserId = "OriginalUserId"
    val properties = new BasicProperties.Builder().messageId(messageId).userId(originalUserId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Reject,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)

    dwh.handle(DeliveryResult.Republish()).await

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(1)).basicAck(deliveryTag, false)
      verify(channel, times(0)).basicReject(deliveryTag, true)
      verify(channel, times(0)).basicReject(deliveryTag, false)
      val propertiesCaptor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel, times(1)).basicPublish(Matchers.eq(""), Matchers.eq("queueName"), propertiesCaptor.capture(), Matchers.eq(body))
      assertResult(Some(originalUserId))(propertiesCaptor.getValue.getHeaders.asScala.get(DefaultRabbitMQConsumer.RepublishOriginalUserId))
    }
  }

  test("should NACK because of unexpected failure") {
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    case class Abc(i: Int)

    implicit val c: DeliveryConverter[Abc] = (_: Bytes) => {
      throw new IllegalArgumentException
    }

    val consumer = new DefaultRabbitMQPullConsumer[Task, Abc](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Retry,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    assertThrows[IllegalArgumentException] {
      consumer.pull().await
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

    val properties = new BasicProperties.Builder().messageId(messageId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    case class Abc(i: Int)

    implicit val c: DeliveryConverter[Abc] = (_: Bytes) => {
      Left(ConversionException(messageId))
    }

    val consumer = new DefaultRabbitMQPullConsumer[Task, Abc](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Ack,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await
    val Delivery.MalformedContent(_, _, _, ce) = dwh.delivery

    assertResult(messageId)(ce.getMessage)

    dwh.handle(DeliveryResult.Retry).await

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      verify(channel, times(0)).basicAck(deliveryTag, false)
      verify(channel, times(1)).basicReject(deliveryTag, true)
    }
  }

  test("passes correlation id") {
    val correlationId = UUID.randomUUID().toString
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder().messageId(messageId).correlationId(correlationId).build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Reject,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)
    assertResult(Some(correlationId))(dwh.delivery.properties.correlationId)
  }

  test("parses correlation id from header") {
    val correlationId = UUID.randomUUID().toString
    val messageId = UUID.randomUUID().toString

    val deliveryTag = Random.nextInt(1000)

    val envelope = mock[Envelope]
    when(envelope.getDeliveryTag).thenReturn(deliveryTag)

    val properties = new BasicProperties.Builder()
      .messageId(messageId)
      .headers(Map(CorrelationIdHeaderName -> correlationId.asInstanceOf[AnyRef]).asJava)
      .build()

    val channel = mock[AutorecoveringChannel]
    when(channel.isOpen).thenReturn(true)

    val body = Random.nextString(5).getBytes

    when(channel.basicGet(Matchers.eq("queueName"), Matchers.eq(false))).thenReturn(
      new GetResponse(envelope, properties, body, 1)
    )

    val consumer = new DefaultRabbitMQPullConsumer[Task, Bytes](
      "test",
      channel,
      "queueName",
      connectionInfo,
      DeliveryResult.Reject,
      Monitor.noOp(),
      RepublishStrategy.DefaultExchange,
      TestBase.testBlocker
    )

    val PullResult.Ok(dwh) = consumer.pull().await

    assertResult(Some(messageId))(dwh.delivery.properties.messageId)
    assertResult(Some(correlationId))(dwh.delivery.properties.correlationId)
  }
}
