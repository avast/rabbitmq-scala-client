package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{CorrelationId, MessageProperties}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}

import scala.util.Random

class DefaultRabbitMQProducerTest extends TestBase {

  test("basic") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val properties = new AMQP.BasicProperties.Builder()
      .contentType("application/octet-stream")
      .build()

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    producer.send(routingKey, body, Some(MessageProperties.empty)).await

    val captor = ArgumentCaptor.forClass(classOf[AMQP.BasicProperties])

    verify(channel, times(1)).basicPublish(Matchers.eq(exchangeName),
                                           Matchers.eq(routingKey),
                                           captor.capture(),
                                           Matchers.eq(body.toByteArray))

    val caughtProperties = captor.getValue

    assert(caughtProperties.getCorrelationId != null)

    val caughtWithoutIds = caughtProperties.builder().messageId(null).correlationId(null).build()

    assertResult(properties.toString)(caughtWithoutIds.toString) // AMQP.BasicProperties doesn't have `equals` method :-/
  }

  test("correlation id is taken from properties") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val cid = Random.nextString(10)
    val cid2 = Random.nextString(10)

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    val mp = MessageProperties(correlationId = Some(cid), headers = Map(CorrelationId.KeyName -> cid2.asInstanceOf[AnyRef]))
    producer.send(routingKey, body, Some(mp)).await

    val captor = ArgumentCaptor.forClass(classOf[AMQP.BasicProperties])

    verify(channel, times(1)).basicPublish(Matchers.eq(exchangeName),
                                           Matchers.eq(routingKey),
                                           captor.capture(),
                                           Matchers.eq(body.toByteArray))

    // check that the one from properties was used
    assertResult(cid)(captor.getValue.getCorrelationId)
  }

  test("correlation id is taken from header if not in properties") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val cid = Random.nextString(10)

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    val mp = MessageProperties(headers = Map(CorrelationId.KeyName -> cid.asInstanceOf[AnyRef]))
    producer.send(routingKey, body, Some(mp)).await

    val captor = ArgumentCaptor.forClass(classOf[AMQP.BasicProperties])

    verify(channel, times(1)).basicPublish(Matchers.eq(exchangeName),
                                           Matchers.eq(routingKey),
                                           captor.capture(),
                                           Matchers.eq(body.toByteArray))

    // check that the one from headers was used
    assertResult(cid)(captor.getValue.getCorrelationId)
  }

  test("correlation id is generated if not in header nor properties") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    producer.send(routingKey, body).await

    val captor = ArgumentCaptor.forClass(classOf[AMQP.BasicProperties])

    verify(channel, times(1)).basicPublish(Matchers.eq(exchangeName),
                                           Matchers.eq(routingKey),
                                           captor.capture(),
                                           Matchers.eq(body.toByteArray))

    // check that some CID was generated
    assert(captor.getValue.getCorrelationId != null)
  }
}
