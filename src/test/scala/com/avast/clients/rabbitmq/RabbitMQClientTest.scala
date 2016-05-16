package com.avast.clients.rabbitmq

import java.time.Duration

import com.avast.metrics.test.NoOpMonitor
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel
import com.typesafe.scalalogging.Logger
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class RabbitMQClientTest extends FunSuite with MockitoSugar with Eventually {
  def createClient(receiver: Option[Logger => Consumer], sender: Option[Delivery => Unit]): RabbitMQClient = {
    new RabbitMQClient(
      name = "test",
      channel = mock[AutorecoveringChannel],
      processTimeout = Duration.ofMillis(200),
      monitor = NoOpMonitor.INSTANCE,
      receiver = receiver,
      sender = sender
    )
  }

  test("initializes receiver") {
    //noinspection ScalaStyle
    var initialized = false

    createClient(Some { logger =>
      initialized = true
      mock[Consumer]
    }, None)

    assertResult(true)(initialized)
  }

  test("sender") {
    val sent = (1 to 50).toSet[Int].map(_ => Random.nextString(10))

    val received = new scala.collection.mutable.HashSet[String]()

    val client = createClient(None, Some { delivery =>
      received += new String(delivery.body)
    })

    sent.foreach { p =>
      client.send("routingKey", p.getBytes)
    }

    assertResult(sent)(received.toSet)
  }
}
