package com.avast.clients.rabbitmq

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{DeliveryResult, MessageProperties}
import com.avast.continuity.Continuity
import com.avast.kluzo.{Kluzo, TraceId}
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import net.ceedubs.ficus.Ficus._
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Seconds, Span}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Success, Try}

class LiveTest extends FunSuite with Eventually {

  import com.avast.clients.rabbitmq.api.DeliveryResult._

  private def createConfig() = new {

    val queueName = randomString(10)
    val exchange1 = randomString(10)
    val exchange2 = randomString(10)

    val queueName2 = randomString(10)
    val exchange3 = randomString(10)

    private val original = ConfigFactory.load().getConfig("myConfig")

    val bindConfigs = original.as[Array[Config]]("consumer.bindings")
    bindConfigs(0) = bindConfigs(0).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
    bindConfigs(1) = bindConfigs(1).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange2))

    val config = original
      .withValue("consumer.queueName", ConfigValueFactory.fromAnyRef(queueName))
      .withValue("consumer.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("consumer.processTimeout", ConfigValueFactory.fromAnyRef("500ms"))
      .withValue("producer.exchange", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("producer2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
      .withValue("additionalDeclarations.declareExchange.name", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("additionalDeclarations.bindExchange.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("additionalDeclarations.bindExchange.destExchangeName", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("additionalDeclarations.declareQueue.name", ConfigValueFactory.fromAnyRef(queueName2))
      .withValue("additionalDeclarations.bindQueue.exchangeName", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("additionalDeclarations.bindQueue.queueName", ConfigValueFactory.fromAnyRef(queueName2))

    def randomString(length: Int): String = {
      Random.alphanumeric.take(length).mkString("")
    }

    implicit val ex = Continuity.wrapExecutionContextExecutorService(ExecutionContext.fromExecutorService(Executors.newCachedThreadPool()))
  }

  val testHelper = new TestHelper(System.getProperty("rabbit.host"), System.getProperty("rabbit.tcp.15672").toInt)

  test("basic") {
    val c = createConfig()
    import c._

    val latch = new CountDownLatch(1)

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      latch.countDown()
      assertResult(true)(Kluzo.getTraceId.nonEmpty)
      Future.successful(DeliveryResult.Ack)
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))

    assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

    assertResult(0)(testHelper.getMessagesCount(queueName))
  }

  test("bunch") {
    val c = createConfig()
    import c._

    val cnt = Random.nextInt(100)
    val count = cnt + 100 // random 100 - 300 messages

    val latch = new CountDownLatch(count + 5)

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val d = new AtomicInteger(0)

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      Future {
        Thread.sleep(if (d.get() % 2 == 0) 300 else 0)
        latch.countDown()

        if (d.incrementAndGet() < (cnt - 50)) Ack
        else {
          if (d.incrementAndGet() < (cnt - 10)) Retry else Republish()
        }
      }
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    for (_ <- 1 to count) {
      sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
    }

    eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
      assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
      assertResult(0)(testHelper.getMessagesCount(queueName))
    }
  }

  test("multiple producers to single consumer") {
    val c = createConfig()
    import c._

    val latch = new CountDownLatch(20)

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      latch.countDown()
      Future.successful(Ack)
    }

    val sender1 = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())
    val sender2 = rabbitFactory.newProducer[Try]("producer2", Monitor.noOp())

    for (_ <- 1 to 10) {
      sender1.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
      sender2.send("test2", Bytes.copyFromUtf8(Random.nextString(10)))
    }

    assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

    assertResult(0)(testHelper.getMessagesCount(queueName))
  }

  test("timeouts and requeues messages") {
    val c = createConfig()
    import c._

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val cnt = new AtomicInteger(0)

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      cnt.incrementAndGet()
      assertResult(true)(Kluzo.getTraceId.nonEmpty)

      Future {
        assertResult(true)(Kluzo.getTraceId.nonEmpty)
        Thread.sleep(800) // timeout is set to 500 ms
        Ack
      }
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    for (_ <- 1 to 10) {
      sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
    }

    eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
      assert(cnt.get() >= 40)
      assert(testHelper.getMessagesCount(queueName) <= 20)
    }
  }

  test("timeouts and requeues messages, blocking the thread") {
    val c = createConfig()
    import c._

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val cnt = new AtomicInteger(0)

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      cnt.incrementAndGet()

      Thread.sleep(800) // timeout is set to 500 ms
      Future.successful(Ack)
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    for (_ <- 1 to 10) {
      sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
    }

    eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
      assert(cnt.get() >= 40)
      assert(testHelper.getMessagesCount(queueName) <= 20)
    }
  }

  test("passes TraceId through the queue") {
    val c = createConfig()
    import c._

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val cnt = new AtomicInteger(0)

    val traceId = TraceId.generate

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      cnt.incrementAndGet()
      assertResult(Some(traceId))(Kluzo.getTraceId)
      Future.successful(Ack)
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    for (_ <- 1 to 10) {
      Kluzo.withTraceId(Some(traceId)) {
        sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
      }
    }

    eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
      assert(cnt.get() == 10)
      assert(testHelper.getMessagesCount(queueName) <= 0)
    }
  }

  test("passes user-specified TraceId through the queue") {
    val c = createConfig()
    import c._

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val cnt = new AtomicInteger(0)

    val traceId = "someTraceId"

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { delivery =>
      cnt.incrementAndGet()
      assertResult(Some(TraceId(traceId)))(Kluzo.getTraceId)
      Future.successful(Ack)
    }

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    for (_ <- 1 to 10) {
      val properties = MessageProperties(headers = Map(Kluzo.HttpHeaderName -> traceId.asInstanceOf[AnyRef]))

      sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)), properties)
    }

    eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
      assert(cnt.get() >= 10)
      assert(testHelper.getMessagesCount(queueName) <= 0)
    }
  }

  test("additional declarations works") {
    val c = createConfig()
    import c._

    val latch = new CountDownLatch(10)

    val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

    val sender = rabbitFactory.newProducer[Try]("producer", Monitor.noOp())

    // additional declarations

    val Success(_) = for {
      _ <- rabbitFactory.declareExchange("additionalDeclarations.declareExchange")
      _ <- rabbitFactory.bindExchange("additionalDeclarations.bindExchange")
      _ <- rabbitFactory.declareQueue("additionalDeclarations.declareQueue")
      _ <- rabbitFactory.bindQueue("additionalDeclarations.bindQueue")
    } yield Done

    rabbitFactory.newConsumer("consumer", Monitor.noOp()) { _ =>
      latch.countDown()
      Future.successful(DeliveryResult.Ack)
    }

    for (_ <- 1 to 10) {
      sender.send("test", Bytes.copyFromUtf8(Random.nextString(10)))
    }

    eventually(timeout(Span(2, Seconds)), interval(Span(200, Milliseconds))) {
      assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

      assertResult(0)(testHelper.getMessagesCount(queueName))
      assertResult(10)(testHelper.getMessagesCount(queueName2))
    }
  }
}
