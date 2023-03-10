package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties, NotAcknowledgedPublish}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.publisher.PublishConfirmsRabbitMQProducer
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicInt
import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Random

class PublisherConfirmsRabbitMQProducerTest extends TestBase {

  test("Message is acked after one retry") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val nextSeqNumber = AtomicInt(0)
    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenAnswer(_ => nextSeqNumber.get())
    when(channel.basicPublish(any(), any(), any(), any())).thenAnswer(_ => {
      nextSeqNumber.increment()
    })

    val producer = new PublishConfirmsRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger,
      sendAttempts = 2
    )

    val body = Bytes.copyFrom(Array.fill(499)(32.toByte))

    val publishFuture = producer.send(routingKey, body).runToFuture

    while (nextSeqNumber.get() < 1) { Thread.sleep(5) }
    producer.DefaultConfirmListener.handleNack(0, multiple = false)

    while (nextSeqNumber.get() < 2) { Thread.sleep(5) }
    producer.DefaultConfirmListener.handleAck(1, multiple = false)

    Await.result(publishFuture, 10.seconds)

    verify(channel, times(2))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  test("Message not acked returned if number of attempts exhausted") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val nextSeqNumber = AtomicInt(0)
    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenAnswer(_ => nextSeqNumber.get())
    when(channel.basicPublish(any(), any(), any(), any())).thenAnswer(_ => {
      nextSeqNumber.increment()
    })

    val producer = new PublishConfirmsRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger,
      sendAttempts = 1
    )

    val body = Bytes.copyFrom(Array.fill(499)(64.toByte))

    val publishTask = producer.send(routingKey, body).runToFuture

    while (nextSeqNumber.get() < 1) {
      Thread.sleep(5)
    }

    producer.DefaultConfirmListener.handleNack(0, multiple = false)

    assertThrows[NotAcknowledgedPublish] {
      Await.result(publishTask, 1.seconds)
    }

    verify(channel).basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  test("Multiple messages are fully acked one by one") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val seqNumbers = (0 to 499).toList
    val nextSeqNumber = AtomicInt(0)

    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenAnswer(_ => nextSeqNumber.get())
    when(channel.basicPublish(any(), any(), any(), any())).thenAnswer(_ => {
      nextSeqNumber.increment()
    })

    val producer = new PublishConfirmsRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger,
      sendAttempts = 2
    )

    val body = Bytes.copyFrom(Array.fill(499)(Random.nextInt(255).toByte))

    val publishFuture = Task.parSequence {
      seqNumbers.map(_ => producer.send(routingKey, body))
    }.runToFuture

    seqNumbers.foreach { seqNumber =>
      while (nextSeqNumber.get() <= seqNumber) { Thread.sleep(5) }
      producer.DefaultConfirmListener.handleAck(seqNumber, multiple = false)
    }

    Await.result(publishFuture, 15.seconds)

    assertResult(seqNumbers.length)(nextSeqNumber.get())
    verify(channel, times(seqNumbers.length))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  test("Multiple messages are fully acked at once") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val messageCount = 500
    val seqNumbers = (0 until messageCount).toList
    val nextSeqNumber = AtomicInt(0)

    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenAnswer(_ => nextSeqNumber.get())
    when(channel.basicPublish(any(), any(), any(), any())).thenAnswer(_ => {
      nextSeqNumber.increment()
    })

    val producer = new PublishConfirmsRabbitMQProducer[Task, Bytes](
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp(),
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      sizeLimitBytes = None,
      blocker = TestBase.testBlocker,
      logger = ImplicitContextLogger.createLogger,
      sendAttempts = 2
    )

    val body = Bytes.copyFrom(Array.fill(499)(Random.nextInt(255).toByte))

    val publishFuture = Task.parSequence {
      seqNumbers.map(_ => producer.send(routingKey, body))
    }.runToFuture

    while (nextSeqNumber.get() < messageCount) { Thread.sleep(5) }
    producer.DefaultConfirmListener.handleAck(messageCount, multiple = true)

    Await.result(publishFuture, 15.seconds)

    assertResult(seqNumbers.length)(nextSeqNumber.get())
    verify(channel, times(seqNumbers.length))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }
}
