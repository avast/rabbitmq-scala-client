package com.avast.clients.rabbitmq

import cats.effect.{ContextShift, IO, Timer}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter
import com.avast.metrics.scalaapi.Monitor
import com.typesafe.config._
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time._

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

class BasicLiveTest extends TestBase with ScalaFutures {
  import pureconfig._

  def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString("")
  }

  private implicit val p: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))

  private lazy val testHelper = new TestHelper(System.getProperty("rabbit.host", System.getenv("rabbit.host")),
                                               System.getProperty("rabbit.tcp.15672", System.getenv("rabbit.tcp.15672")).toInt)

  //noinspection ScalaStyle
  private def createConfig() = new {
    val queueName1: String = randomString(4) + "_QU1"
    val queueName2: String = randomString(4) + "_QU2"
    val exchange1: String = randomString(4) + "_EX1"
    val exchange2: String = randomString(4) + "_EX2"
    val exchange3: String = randomString(4) + "_EX3"
    val exchange4: String = randomString(4) + "_EX4"
    val exchange5: String = randomString(4) + "_EX5"

    testHelper.queue.delete(queueName1)
    testHelper.queue.delete(queueName2)

    private val original = ConfigFactory.load().getConfig("myConfig")

    val bindConfigs: Array[Config] = original.getObjectList("consumers.testing.bindings").asScala.map(_.toConfig).toArray
    bindConfigs(0) = bindConfigs(0).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
    bindConfigs(1) = bindConfigs(1).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange2))

    val config: Config = original
      .withValue("republishStrategy.exchangeName", ConfigValueFactory.fromAnyRef(exchange5))
      .withValue("consumers.testing.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testing.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("consumers.testingPull.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingPull.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("producers.testing.exchange", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("producers.testing2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
      .withValue("producers.testing3.exchange", ConfigValueFactory.fromAnyRef(exchange4))
      .withValue("declarations.foo.declareExchange.name", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("declarations.bindExchange1.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange4))
      .withValue("declarations.bindExchange1.destExchangeName", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("declarations.bindExchange2.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange4))
      .withValue("declarations.bindExchange2.destExchangeName", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("declarations.declareQueue.name", ConfigValueFactory.fromAnyRef(queueName2))
      .withValue("declarations.bindQueue.exchangeName", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("declarations.bindQueue.queueName", ConfigValueFactory.fromAnyRef(queueName2))

    val ex: ExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    implicit val sched: Scheduler = Scheduler(
      Executors.newScheduledThreadPool(4),
      ExecutionContext.fromExecutor(new ForkJoinPool())
    )
  }

  test("basic") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val counter = new AtomicInteger(0)
      val processed = new Semaphore(0)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        counter.incrementAndGet()
        Task {
          processed.release()
          DeliveryResult.Ack
        }
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await

          assert(processed.tryAcquire(1, TimeUnit.SECONDS)) // this is to prevent bug where the event was processed multiple times

          eventually {
            assertResult(1)(counter.get())
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          }
        }
      }
    }
  }

  test("bunch") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val count = Random.nextInt(2000) + 2000 // 2-4k

      logger.info(s"Sending $count messages")

      val latch = new CountDownLatch(count + 100) // explanation below

      val d = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        Task(d.incrementAndGet()).flatMap { n =>
          Task.sleep((if (n % 5 == 0) 150 else 0).millis) >>
            Task {
              latch.countDown()

              if (n < (count - 100) || n > count) Ack else Retry

              // ^ example: 750 messages in total => 650 * Ack, 100 * Retry => processing 850 (== +100) messages in total
            }
        }
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to count) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          // it takes some time before the stats appear... :-|
          eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
            assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
          }

          eventually(timeout(Span(120, Seconds)), interval(Span(2, Seconds))) {
            val inQueue = testHelper.queue.getMessagesCount(queueName1)
            println(s"In QUEUE COUNT: $inQueue")
            assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
            assertResult(0)(inQueue)
          }
        }
      }
    }
  }

  test("multiple producers to single consumer") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val latch = new CountDownLatch(20)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        latch.countDown()
        Task.now(Ack)
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender1 =>
          rabbitConnection.newProducer[Bytes]("testing2", Monitor.noOp()).withResource { sender2 =>
            for (_ <- 1 to 10) {
              sender1.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await(500.millis)
              sender2.send("test2", Bytes.copyFromUtf8(Random.nextString(10))).await(500.millis)
            }

            assertResult(true, latch.getCount)(latch.await(1000, TimeUnit.MILLISECONDS))
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          }
        }
      }
    }
  }

  test("timeouts and requeues messages") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val cnt = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        cnt.incrementAndGet()

        Task {
          Ack
        }.delayResult(800.millis) // timeout is set to 500 ms
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to 10) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          eventually(timeout(Span(3, Seconds)), interval(Span(0.5, Seconds))) {
            assert(cnt.get() >= 40)
            assert(testHelper.queue.getMessagesCount(queueName1) <= 20)
          }
        }
      }
    }
  }

  test("timeouts and requeues messages, blocking the thread") {
    val c = createConfig()
    import c._

    implicit val cs: ContextShift[IO] = IO.contextShift(TestBase.testBlockingScheduler)
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    RabbitMQConnection.fromConfig[IO](config, ex).withResource { rabbitConnection =>
      val cnt = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        cnt.incrementAndGet()
        Thread.sleep(800) // timeout is set to 500 ms
        IO.pure(Ack)
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to 10) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
            assert(cnt.get() >= 40)
            assert(testHelper.queue.getMessagesCount(queueName1) <= 20)
          }
        }
      }
    }
  }

  test("additional declarations works") {
    val c = createConfig()
    import c._

    /*
      -- > EXCHANGE4 ---(test) --> EXCHANGE3 --(test)--> QUEUE2
                     |
                     |--(test) --> EXCHANGE1 --(test)--> QUEUE1
     */

    implicit val cs: ContextShift[IO] = IO.contextShift(TestBase.testBlockingScheduler)
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    RabbitMQConnection.fromConfig[IO](config, ex).withResource { rabbitConnection =>
      val latch = new CountDownLatch(10)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        latch.countDown()
        IO.pure(Ack)
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing3", Monitor.noOp()).withResource { sender =>
          // additional declarations

          (for { // the order consumer -> producer -> declarations is required!
            _ <- rabbitConnection.declareExchange("foo.declareExchange")
            _ <- rabbitConnection.bindExchange("bindExchange1")
            _ <- rabbitConnection.bindExchange("bindExchange2")
            _ <- rabbitConnection.declareQueue("declareQueue")
            _ <- rabbitConnection.bindQueue("bindQueue")
          } yield ()).unsafeRunSync()

          assertResult(Map("x-max-length" -> 1000000))(testHelper.queue.getArguments(queueName2))

          assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          assertResult(0)(testHelper.queue.getMessagesCount(queueName2))

          for (_ <- 1 to 10) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          eventually(timeout(Span(2, Seconds)), interval(Span(200, Milliseconds))) {
            assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))

            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
            assertResult(10)(testHelper.queue.getMessagesCount(queueName2))
          }
        }
      }
    }
  }

  test("pull consumer") {
    val c = createConfig()
    import c._

    implicit val cs: ContextShift[IO] = IO.contextShift(TestBase.testBlockingScheduler)
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    RabbitMQConnection.fromConfig[IO](config, ex).withResource { rabbitConnection =>
      val cons = rabbitConnection.newPullConsumer[Bytes]("testingPull", Monitor.noOp())

      cons.withResource { consumer =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to 10) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          eventually(timeout = timeout(Span(5, Seconds))) {
            assertResult(10)(testHelper.queue.getMessagesCount(queueName1))
          }

          for (_ <- 1 to 3) {
            val PullResult.Ok(dwh) = consumer.pull().await
            dwh.handle(DeliveryResult.Ack).await
          }

          eventually(timeout = timeout(Span(5, Seconds))) {
            assertResult(7)(testHelper.queue.getMessagesCount(queueName1))
          }

          for (_ <- 1 to 7) {
            val PullResult.Ok(dwh) = consumer.pull().await
            dwh.handle(DeliveryResult.Ack).await
          }

          eventually(timeout = timeout(Span(5, Seconds))) {
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          }

          for (_ <- 1 to 10) {
            assertResult(PullResult.EmptyQueue)(consumer.pull().await)
          }
        }
      }
    }
  }

  test("consumer parsing failure") {
    val c = createConfig()
    import c._
    import io.circe.generic.auto._
    case class Abc(str: String)

    implicit val conv: DeliveryConverter[Abc] = JsonDeliveryConverter.derive[Abc]()

    implicit val cs: ContextShift[IO] = IO.contextShift(TestBase.testBlockingScheduler)
    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    RabbitMQConnection.fromConfig[IO](config, ex).withResource { rabbitConnection =>
      val parsingFailures = new AtomicInteger(0)
      val processing = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer[Abc]("testing", Monitor.noOp()) {
        case _: Delivery.Ok[Abc] =>
          processing.incrementAndGet()
          IO(DeliveryResult.Ack)

        case d: Delivery.MalformedContent =>
          assertResult(10)(d.body.size())

          val i = parsingFailures.incrementAndGet()
          IO {
            if (i > 3) DeliveryResult.Ack
            else {
              logger.info(s"Retrying $i", d.ce)
              DeliveryResult.Retry
            }
          }
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          sender.send("test", Bytes.copyFromUtf8(randomString(10))).await

          eventually(timeout = timeout(Span(5, Seconds))) {
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))

            assertResult(0)(processing.get())
            assertResult(4)(parsingFailures.get())

          }
        }
      }
    }
  }

  test("custom exchange republish strategy works") {
    val c = createConfig()
    import c._

    val count = 100

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val counter = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer("testing", Monitor.noOp()) { _: Delivery[Bytes] =>
        val c = counter.incrementAndGet()
        Task {
          if (c <= 10) DeliveryResult.Republish() else DeliveryResult.Ack // republish first 10 messages
        }
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to count) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          eventually {
            assertResult(count + 10)(counter.get())
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
            assertResult(count + 10)(testHelper.queue.getPublishedCount(queueName1))
            assertResult(10)(testHelper.exchange.getPublishedCount(exchange5))
          }
        }
      }
    }
  }

  test("propagates correlation ID") {
    val c = createConfig()
    import c._

    val messageCount = 10

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      val cons = rabbitConnection.newConsumer[Bytes]("testing", Monitor.noOp()) {
        case Delivery.Ok(body, properties, _) =>
          assertResult(Some(body.toStringUtf8))(properties.correlationId)
          processed.incrementAndGet()
          Task.now(DeliveryResult.Ack)

        case _ => fail("malformed")
      }

      cons.withResource { _ =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (i <- 1 to messageCount) {
            implicit val cid: CorrelationId = CorrelationId(s"cid$i")
            sender.send("test", Bytes.copyFromUtf8(s"cid$i")).await
          }

          eventually {
            assertResult(messageCount)(processed.get())
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          }
        }
      }
    }
  }
}
