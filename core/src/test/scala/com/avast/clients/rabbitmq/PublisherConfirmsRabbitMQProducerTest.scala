package com.avast.clients.rabbitmq

import cats.effect.concurrent.{Deferred, Ref}
import cats.syntax.parallel._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties, NotAcknowledgedPublish}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.publisher.PublishConfirmsRabbitMQProducer
import com.avast.clients.rabbitmq.publisher.PublishConfirmsRabbitMQProducer.SentMessages
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}

import scala.util.Random

class PublisherConfirmsRabbitMQProducerTest extends TestBase {
  test("message is acked after one retry") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)
    val seqNumber = 1L
    val seqNumber2 = 2L

    val channel = mock[AutorecoveringChannel]
    val ref = Ref.of[Task, Map[Long, Deferred[Task, Either[NotAcknowledgedPublish, Unit]]]](Map.empty).await
    val updatedState1 = updateMessageState(ref, seqNumber)(Left(NotAcknowledgedPublish("abcd", messageId = seqNumber)))
    val updatedState2 = updateMessageState(ref, seqNumber2)(Right())

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
      sentMessages = ref,
      sendAttempts = 2
    )
    when(channel.getNextPublishSeqNo).thenReturn(seqNumber, seqNumber2)

    producer.send(routingKey, Bytes.copyFrom(Array.fill(499)(32.toByte))).parProduct(updatedState1.parProduct(updatedState2)).await

    verify(channel, times(2))
      .basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(Bytes.copyFrom(Array.fill(499)(32.toByte)).toByteArray))
  }

  test("Message not acked returned if number of attempts exhausted") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)
    val seqNumber = 1L

    val channel = mock[AutorecoveringChannel]
    val ref = Ref.of[Task, Map[Long, Deferred[Task, Either[NotAcknowledgedPublish, Unit]]]](Map.empty).await
    val updatedState = updateMessageState(ref, seqNumber)(Left(NotAcknowledgedPublish("abcd", messageId = seqNumber)))

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
      sentMessages = ref,
      sendAttempts = 1
    )
    when(channel.getNextPublishSeqNo).thenReturn(seqNumber)

    assertThrows[NotAcknowledgedPublish] {
      producer.send(routingKey, Bytes.copyFrom(Array.fill(499)(32.toByte))).parProduct(updatedState).await
    }

    verify(channel).basicPublish(Matchers.eq(exchangeName), Matchers.eq(routingKey), any(), Matchers.eq(Bytes.copyFrom(Array.fill(499)(32.toByte)).toByteArray))
  }

  private def updateMessageState(ref: SentMessages[Task], messageId: Long)(result: Either[NotAcknowledgedPublish, Unit]): Task[Unit] = {
    ref.get.flatMap(map => map.get(messageId) match {
      case Some(value) => value.complete(result)
      case None => updateMessageState(ref, messageId)(result)
    })
  }
}
