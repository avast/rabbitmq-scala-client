package com.avast.clients.rabbitmq

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.avast.metrics.test.NoOpMonitor
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import net.ceedubs.ficus.Ficus._
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class LiveTest extends FunSuite with Eventually {
  private def createConfig() = new {

    val queueName = randomString(10)
    val exchange1 = randomString(10)
    val exchange2 = randomString(10)

    private val original = ConfigFactory.load().getConfig("myConfig")

    val bindConfigs = original.as[Array[Config]]("consumer.binds")
    bindConfigs(0) = bindConfigs(0).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
    bindConfigs(1) = bindConfigs(1).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange2))


    val config = original
      .withValue("consumer.queueName", ConfigValueFactory.fromAnyRef(queueName))
      .withValue("consumer.binds", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("producer.exchange", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("producer2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
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

  test("multiple producers to single consumer") {
    val c = createConfig()
    import c._

    implicit val ex = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    val latch = new CountDownLatch(20)

    val channelFactory = RabbitMQChannelFactory.fromConfig(config, Some(ex))

    RabbitMQClientFactory.Consumer.fromConfig(config.getConfig("consumer"), channelFactory, NoOpMonitor.INSTANCE) { delivery =>
      latch.countDown()
      Future.successful(true)
    }

    val sender1 = RabbitMQClientFactory.Producer.fromConfig(config.getConfig("producer"), channelFactory, NoOpMonitor.INSTANCE)

    val sender2 = RabbitMQClientFactory.Producer.fromConfig(config.getConfig("producer2"), channelFactory, NoOpMonitor.INSTANCE)

    for (_ <- 1 to 10) {
      sender1.send("test", Random.nextString(10).getBytes())
      sender2.send("test2", Random.nextString(10).getBytes())
    }

    assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

    assertResult(0)(testHelper.getMessagesCount(queueName))
  }
}
