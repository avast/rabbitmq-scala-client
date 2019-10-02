package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.MessageProperties
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.eval.Task
import monix.execution.Scheduler
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
      monitor = Monitor.noOp,
      defaultProperties = MessageProperties.empty,
      reportUnroutable = false,
      blocker = TestBase.testBlocker
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

    assertResult(properties.toString)(captor.getValue.toString) // AMQP.BasicProperties doesn't have `equals` method :-/
  }

  // todo add more tests
}
