package com.avast.clients.rabbitmq

import cats.implicits.catsSyntaxParallelAp
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties, NotAcknowledgedPublish}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.publisher.PublishConfirmsRabbitMQProducer
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.manipulation.InvalidOrderingException
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
    val seqNumber = 1L
    val seqNumber2 = 2L

    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenReturn(seqNumber, seqNumber2)

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

    val publishTask = producer.send(routingKey, body).runToFuture

    updateMessageState(producer, seqNumber)(Left(NotAcknowledgedPublish("abcd", messageId = seqNumber))).parProduct {
      updateMessageState(producer, seqNumber2)(Right())
    }.await

    Await.result(publishTask, 10.seconds)

    verify(channel, times(2))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  test("Message not acked returned if number of attempts exhausted") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)
    val seqNumber = 1L

    val channel = mock[AutorecoveringChannel]
    when(channel.getNextPublishSeqNo).thenReturn(seqNumber)

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

    val body = Bytes.copyFrom(Array.fill(499)(32.toByte))

    val publishTask = producer.send(routingKey, body).runToFuture

    assertThrows[NotAcknowledgedPublish] {
      updateMessageState(producer, seqNumber)(Left(NotAcknowledgedPublish("abcd", messageId = seqNumber))).await
      Await.result(publishTask, 10.seconds)
    }

    verify(channel).basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  test("Multiple messages are fully acked") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val seqNumbers = 1 to 500
    val iterator = seqNumbers.iterator
    when(channel.getNextPublishSeqNo).thenAnswer(_ => { iterator.next() })

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

    val publishTasks = Task.parSequenceUnordered {
      seqNumbers.map { _ =>
        producer.send(routingKey, body)
      }
    }.runToFuture

    Task
      .parSequenceUnordered(seqNumbers.map { seqNumber =>
        updateMessageState(producer, seqNumber)(Right())
      })
      .await(15.seconds)

    Await.result(publishTasks, 15.seconds)

    verify(channel, times(seqNumbers.length))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(body.toByteArray))
  }

  private def updateMessageState(producer: PublishConfirmsRabbitMQProducer[Task, Bytes], messageId: Long, attempt: Int = 1)(
      result: Either[NotAcknowledgedPublish, Unit]): Task[Unit] = {
    Task
      .delay(producer.confirmationCallbacks.get(messageId))
      .flatMap {
        case Some(value) => value.complete(result)
        case None =>
          if (attempt < 90) {
            Task.sleep(100.millis) >> updateMessageState(producer, messageId, attempt + 1)(result)
          } else {
            throw new InvalidOrderingException(s"The message ID $messageId is not present in the list of callbacks")
          }
      }
  }
}
