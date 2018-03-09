package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.MessageProperties
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.mockito.MockitoSugar

import scala.util.Random

class DefaultRabbitMQProducerTest extends FunSuite with MockitoSugar with Eventually with ScalaFutures {
  test("basic") {
    val exchangeName = Random.nextString(10)
    val routingKey = Random.nextString(10)

    val channel = mock[AutorecoveringChannel]

    val producer = new DefaultRabbitMQProducer(
      name = "test",
      exchangeName = exchangeName,
      channel = channel,
      monitor = Monitor.noOp,
      useKluzo = true,
      reportUnroutable = false
    )

    val properties = new AMQP.BasicProperties.Builder()
      .build()

    val body = Bytes.copyFromUtf8(Random.nextString(10))

    producer.send(routingKey, body, MessageProperties.empty).runAsync.futureValue

    val captor = ArgumentCaptor.forClass(classOf[AMQP.BasicProperties])

    verify(channel, times(1)).basicPublish(Matchers.eq(exchangeName),
                                           Matchers.eq(routingKey),
                                           captor.capture(),
                                           Matchers.eq(body.toByteArray))

    assertResult(properties.toString)(captor.getValue.toString) // AMQP.BasicProperties doesn't have `equals` method :-/
  }

  // todo add more tests
}
