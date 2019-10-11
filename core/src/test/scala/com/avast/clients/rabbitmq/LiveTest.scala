//package com.avast.clients.rabbitmq
//
//import java.util.concurrent._
//import java.util.concurrent.atomic.AtomicInteger
//
//import cats.effect.SyncIO
//import com.avast.bytes.Bytes
//import com.avast.clients.rabbitmq.api._
//import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler
//import com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter
//import com.avast.clients.rabbitmq.extras.testing._
//import com.avast.metrics.scalaapi.Monitor
//import com.typesafe.config._
//import monix.eval.Task
//import monix.execution.Scheduler
//import net.ceedubs.ficus.Ficus._
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.time._
//
//import scala.collection.JavaConverters._
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import scala.util.Random
//
//class LiveTest extends TestBase with ScalaFutures {
//
//  import com.avast.clients.rabbitmq.api.DeliveryResult._
//
//  private implicit val p: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))
//
//  private def createConfig() = new {
//
//    val queueName: String = randomString(10)
//    val exchange1: String = randomString(10)
//    val exchange2: String = randomString(10)
//
//    val queueName2: String = randomString(10)
//    val exchange3: String = randomString(10)
//
//    private val original = ConfigFactory.load().getConfig("myConfig")
//
//    val bindConfigs: Array[Config] = original.as[Array[Config]]("consumer.bindings")
//    bindConfigs(0) = bindConfigs(0).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
//    bindConfigs(1) = bindConfigs(1).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange2))
//
//    val config: Config = original
//      .withValue("consumer.queueName", ConfigValueFactory.fromAnyRef(queueName))
//      .withValue("consumer.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
//      .withValue("consumer.processTimeout", ConfigValueFactory.fromAnyRef("500ms"))
//      .withValue("producer.exchange", ConfigValueFactory.fromAnyRef(exchange1))
//      .withValue("producer2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
//      .withValue("additionalDeclarations.declareExchange.name", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("additionalDeclarations.bindExchange.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange1))
//      .withValue("additionalDeclarations.bindExchange.destExchangeName", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("additionalDeclarations.declareQueue.name", ConfigValueFactory.fromAnyRef(queueName2))
//      .withValue("additionalDeclarations.bindQueue.exchangeName", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("additionalDeclarations.bindQueue.queueName", ConfigValueFactory.fromAnyRef(queueName2))
//
//    def randomString(length: Int): String = {
//      Random.alphanumeric.take(length).mkString("")
//    }
//
//    val ex: ExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
//
//    implicit val sched: Scheduler = Scheduler(Executors.newCachedThreadPool())
//  }
//
//  val testHelper = new TestHelper(System.getProperty("rabbit.host", System.getenv("rabbit.host")),
//                                  System.getProperty("rabbit.tcp.15672", System.getenv("rabbit.tcp.15672")).toInt)
//
//  test("basic") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val counter = new AtomicInteger(0)
//      val processed = new Semaphore(0)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        counter.incrementAndGet()
//        Task {
//          processed.release()
//          DeliveryResult.Ack
//        }
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//
//          assert(processed.tryAcquire(1, TimeUnit.SECONDS)) // this is to prevent bug where the event was processed multiple times
//
//          eventually {
//            assertResult(1)(counter.get())
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//          }
//        }
//      }
//    }
//  }
//
//  test("bunch") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val cnt = Random.nextInt(100)
//      val count = cnt + 100 // random 100 - 300 messages
//
//      val latch = new CountDownLatch(count + 5)
//
//      val d = new AtomicInteger(0)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        Task {
//          Thread.sleep(if (d.get() % 2 == 0) 300 else 0)
//          latch.countDown()
//
//          if (d.incrementAndGet() < (cnt - 50)) Ack
//          else {
//            if (d.incrementAndGet() < (cnt - 10)) Retry else Republish()
//          }
//        }
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to count) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//          }
//
//          eventually(timeout(Span(3, Seconds)), interval(Span(0.1, Seconds))) {
//            assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//          }
//        }
//      }
//    }
//  }
//
//  test("multiple producers to single consumer") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val latch = new CountDownLatch(20)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        latch.countDown()
//        Task.now(Ack)
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender1 =>
//          rabbitConnection.newProducer[Bytes]("producer2", Monitor.noOp()).withResource { sender2 =>
//            for (_ <- 1 to 10) {
//              sender1.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await(500.millis)
//              sender2.send("test2", Bytes.copyFromUtf8(Random.nextString(10))).await(500.millis)
//            }
//
//            assertResult(true, latch.getCount)(latch.await(1000, TimeUnit.MILLISECONDS))
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//          }
//        }
//      }
//    }
//  }
//
//  test("timeouts and requeues messages") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val cnt = new AtomicInteger(0)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        cnt.incrementAndGet()
//
//        Task {
//          Ack
//        }.delayResult(800.millis) // timeout is set to 500 ms
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to 10) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//          }
//
//          eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
//            assert(cnt.get() >= 40)
//            assert(testHelper.getMessagesCount(queueName) <= 20)
//          }
//        }
//      }
//    }
//  }
//
//  test("timeouts and requeues messages, blocking the thread") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).map(_.asBlocking).withResource { rabbitConnection =>
//      val cnt = new AtomicInteger(0)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        cnt.incrementAndGet()
//        Thread.sleep(800) // timeout is set to 500 ms
//        SyncIO.pure(Ack)
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to 10) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).unsafeRunSync()
//          }
//
//          eventually(timeout(Span(5, Seconds)), interval(Span(0.25, Seconds))) {
//            assert(cnt.get() >= 40)
//            assert(testHelper.getMessagesCount(queueName) <= 20)
//          }
//        }
//      }
//    }
//  }
//
//  test("additional declarations works") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).map(_.asBlocking).withResource { rabbitConnection =>
//      val latch = new CountDownLatch(10)
//
//      val cons = rabbitConnection.newConsumer("consumer", Monitor.noOp()) { _: Delivery[Bytes] =>
//        latch.countDown()
//        SyncIO.pure(Ack)
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          // additional declarations
//
//          (for { // the order consumer -> producer -> declarations is required!
//            _ <- rabbitConnection.declareExchange("additionalDeclarations.declareExchange")
//            _ <- rabbitConnection.bindExchange("additionalDeclarations.bindExchange")
//            _ <- rabbitConnection.declareQueue("additionalDeclarations.declareQueue")
//            _ <- rabbitConnection.bindQueue("additionalDeclarations.bindQueue")
//          } yield ()).unsafeRunSync()
//
//          for (_ <- 1 to 10) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).unsafeRunSync()
//          }
//
//          eventually(timeout(Span(2, Seconds)), interval(Span(200, Milliseconds))) {
//            assertResult(true)(latch.await(500, TimeUnit.MILLISECONDS))
//
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//            assertResult(10)(testHelper.getMessagesCount(queueName2))
//          }
//        }
//      }
//    }
//  }
//
//  test("PoisonedMessageHandler") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val poisoned = new AtomicInteger(0)
//      val processed = new AtomicInteger(0)
//
//      val h = PoisonedMessageHandler.withCustomPoisonedAction[Task, Bytes](2) { _: Delivery[Bytes] =>
//        Task {
//          processed.incrementAndGet()
//          DeliveryResult.Republish()
//        }
//      } { _: Delivery[Bytes] =>
//        Task {
//          poisoned.incrementAndGet()
//          ()
//        }
//      }
//
//      rabbitConnection.newConsumer("consumer", Monitor.noOp())(h).withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to 10) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//          }
//
//          eventually(timeout(Span(2, Seconds)), interval(Span(0.25, Seconds))) {
//            assertResult(20)(processed.get())
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//            assertResult(10)(poisoned.get())
//          }
//        }
//      }
//    }
//  }
//
//  test("pull consumer") {
//    val c = createConfig()
//    import c._
//
//    RabbitMQConnection.fromConfig[Task](config, ex).map(_.asBlocking).withResource { rabbitConnection =>
//      val cons = rabbitConnection.newPullConsumer[Bytes]("consumer", Monitor.noOp())
//
//      cons.withResource { consumer =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to 10) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).unsafeRunSync()
//          }
//
//          eventually(timeout = timeout(Span(5, Seconds))) {
//            assertResult(10)(testHelper.getMessagesCount(queueName))
//          }
//
//          for (_ <- 1 to 3) {
//            val PullResult.Ok(dwh) = consumer.pull().unsafeRunSync()
//            dwh.handle(DeliveryResult.Ack).unsafeRunSync()
//          }
//
//          eventually(timeout = timeout(Span(5, Seconds))) {
//            assertResult(7)(testHelper.getMessagesCount(queueName))
//          }
//
//          for (_ <- 1 to 7) {
//            val PullResult.Ok(dwh) = consumer.pull().unsafeRunSync()
//            dwh.handle(DeliveryResult.Ack).unsafeRunSync()
//          }
//
//          eventually(timeout = timeout(Span(5, Seconds))) {
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//          }
//
//          for (_ <- 1 to 10) {
//            assertResult(PullResult.EmptyQueue)(consumer.pull().unsafeRunSync())
//          }
//        }
//      }
//    }
//  }
//
//  test("consumer parsing failure") {
//    val c = createConfig()
//    import c._
//    import io.circe.generic.auto._
//    case class Abc(str: String)
//
//    implicit val conv: DeliveryConverter[Abc] = JsonDeliveryConverter.derive[Abc]()
//
//    RabbitMQConnection.fromConfig[Task](config, ex).map(_.asBlocking).withResource { rabbitConnection =>
//      val parsingFailures = new AtomicInteger(0)
//      val processing = new AtomicInteger(0)
//
//      val cons = rabbitConnection.newConsumer[Abc]("consumer", Monitor.noOp()) {
//        case _: Delivery.Ok[Abc] =>
//          processing.incrementAndGet()
//          SyncIO(DeliveryResult.Ack)
//
//        case d: Delivery.MalformedContent =>
//          assertResult(10)(d.body.size())
//
//          val i = parsingFailures.incrementAndGet()
//          SyncIO {
//            if (i > 3) DeliveryResult.Ack
//            else {
//              logger.info(s"Retrying $i", d.ce)
//              DeliveryResult.Retry
//            }
//          }
//      }
//
//      cons.withResource { _ =>
//        rabbitConnection.newProducer[Bytes]("producer", Monitor.noOp()).withResource { sender =>
//          sender.send("test", Bytes.copyFromUtf8(randomString(10))).unsafeRunSync()
//
//          eventually(timeout = timeout(Span(5, Seconds))) {
//            assertResult(0)(testHelper.getMessagesCount(queueName))
//
//            assertResult(0)(processing.get())
//            assertResult(4)(parsingFailures.get())
//
//          }
//        }
//      }
//    }
//  }
//}
