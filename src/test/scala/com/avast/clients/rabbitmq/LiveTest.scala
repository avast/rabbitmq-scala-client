package com.avast.clients.rabbitmq

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.avast.metrics.test.NoOpMonitor
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class LiveTest extends FunSuite with Eventually {
  private def createConfig() = new {

    val queueName = randomString(10)
    val exchange = randomString(10)

    val config = ConfigFactory.load().getConfig("myConfig")
      .withValue("consumer.queueName", ConfigValueFactory.fromAnyRef(queueName))
      .withValue("consumer.bind.exchange.name", ConfigValueFactory.fromAnyRef(exchange))
      .withValue("producer.exchange", ConfigValueFactory.fromAnyRef(exchange))
  }

  def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString("")
  }

  val testHelper = new TestHelper(System.getProperty("rabbit.host"), System.getProperty("rabbit.tcp.15672").toInt)

  test("basic") {
    val c = createConfig()
    import c._

    implicit val ex = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    val latch = new CountDownLatch(1)

    val channelFactory = RabbitMQChannelFactory.fromConfig(config, Some(ex))

    RabbitMQClientFactory.Consumer.fromConfig(config.getConfig("consumer"), channelFactory, NoOpMonitor.INSTANCE) { delivery =>
      latch.countDown()
      Future.successful(true)
    }

    val sender = RabbitMQClientFactory.Producer.fromConfig(config.getConfig("producer"), channelFactory, NoOpMonitor.INSTANCE)


    sender.send("test", Random.nextString(10).getBytes())

    assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

    assertResult(0)(testHelper.getMessagesCount(queueName))
  }

  test("bunch") {
    val c = createConfig()
    import c._

    implicit val ex = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    val cnt = Random.nextInt(100)
    val count = cnt + 100 // random 100 - 300 messages

    val latch = new CountDownLatch(count + 5)

    val channelFactory = RabbitMQChannelFactory.fromConfig(config, Some(ex))

    val d = new AtomicInteger(0)

    RabbitMQClientFactory.Consumer.fromConfig(config.getConfig("consumer"), channelFactory, NoOpMonitor.INSTANCE) { delivery =>
      Future {
        Thread.sleep(if (d.get() % 2 == 0) 300 else 0)
        latch.countDown()

        d.incrementAndGet() > 5
      }
    }

    val sender = RabbitMQClientFactory.Producer.fromConfig(config.getConfig("producer"), channelFactory, NoOpMonitor.INSTANCE)

    for (_ <- 1 to count) {
      sender.send("test", Random.nextString(10).getBytes())
    }

    eventually(timeout(Span(1, Seconds)), interval(Span(0.1, Seconds))) {
      assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
      assertResult(0)(testHelper.getMessagesCount(queueName))
    }
  }
}
