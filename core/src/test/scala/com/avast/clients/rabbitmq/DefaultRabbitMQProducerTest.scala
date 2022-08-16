package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.{AMQP, ConfirmCallback, ConfirmListener}
import com.rabbitmq.client.AMQP.Confirm
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentCaptor, Matchers}

import scala.util.Random

class DefaultRabbitMQProducerTest extends TestBase {

  // default, is overridden in some tests
  implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.RandomNew

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
      sizeLimitBytes = None,
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
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val cid = Random.nextString(10)
    val cid2 = Random.nextString(10)

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    val mp = Some(
      MessageProperties(correlationId = Some(cid), headers = Map(CorrelationIdStrategy.CorrelationIdKeyName -> cid2.asInstanceOf[AnyRef]))
    )

    implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.FromPropertiesOrRandomNew(mp)

    producer.send(routingKey, body, mp).await

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
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    val cid = Random.nextString(10)

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    val mp = Some(MessageProperties(headers = Map(CorrelationIdStrategy.CorrelationIdKeyName -> cid.asInstanceOf[AnyRef])))

    implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.FromPropertiesOrRandomNew(mp)

    producer.send(routingKey, body, mp).await

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
      sizeLimitBytes = None,
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

  test("too big message is denied") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val limit = 500

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = Some(limit),
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    // don't test anything except it doesn't fail
    producer.send(routingKey, Bytes.copyFrom(Array.fill(499)(32.toByte))).await

    assertThrows[TooBigMessage] {
      producer.send(routingKey, Bytes.copyFrom(Array.fill(501)(32.toByte))).await
    }

    assertThrows[TooBigMessage] {
      producer.send(routingKey, Bytes.copyFrom(Array.fill(Random.nextInt(1000) + 500)(32.toByte))).await
    }
  }

  test("counts ACKs and NACKs") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val monitor = new TestMonitor[Task]
    val channel = mock[AutorecoveringChannel]

    when(channel.confirmSelect()).thenReturn(new Confirm.SelectOk.Builder().build())

    var ackCallback: ConfirmCallback = null
    var nackCallback: ConfirmCallback = null

    when(channel.addConfirmListener(any[ConfirmCallback], any[ConfirmCallback])).thenAnswer((invocation: InvocationOnMock) => {
      ackCallback = invocation.getArgumentAt(0, classOf[ConfirmCallback])
      nackCallback = invocation.getArgumentAt(1, classOf[ConfirmCallback])
      new ConfirmListener {
        override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = ()
        override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = ()
      }
    })

    val producer = new DefaultRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = monitor,
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger
    )

    // not needed to actually send anything...

    val acks = Random.nextInt(100)
    val nacks = Random.nextInt(100)

    for (_ <- 1 to acks) ackCallback.handle(0, true)
    for (_ <- 1 to nacks) nackCallback.handle(0, true)

    assertResult(acks)(monitor.registry.meterCount("ack"))
    assertResult(nacks)(monitor.registry.meterCount("nack"))
  }
}
