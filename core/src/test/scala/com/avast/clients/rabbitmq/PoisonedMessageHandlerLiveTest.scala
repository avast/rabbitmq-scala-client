//package com.avast.clients.rabbitmq.extras
//
//import com.avast.bytes.Bytes
//import com.avast.clients.rabbitmq.RabbitMQConnection
//import com.avast.clients.rabbitmq.api._
//import com.avast.clients.rabbitmq.pureconfig._
//import com.avast.metrics.scalaapi.Monitor
//import com.typesafe.config._
//import monix.eval.Task
//import monix.execution.Scheduler
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.time._
//
//import java.util.concurrent._
//import java.util.concurrent.atomic.AtomicInteger
//import scala.concurrent.ExecutionContext
//import scala.concurrent.duration._
//import scala.jdk.CollectionConverters._
//import scala.util.Random
//
//class PoisonedMessageHandlerLiveTest extends TestBase with ScalaFutures {
//
//  def randomString(length: Int): String = {
//    Random.alphanumeric.take(length).mkString("")
//  }
//
//  private implicit val p: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))
//
//  private lazy val testHelper = new TestHelper(System.getProperty("rabbit.host", System.getenv("rabbit.host")),
//                                               System.getProperty("rabbit.tcp.15672", System.getenv("rabbit.tcp.15672")).toInt)
//
//  //noinspection ScalaStyle
//  private def createConfig() = new {
//    val queueName1: String = randomString(4) + "_QU1"
//    val queueName2: String = randomString(4) + "_QU2"
//    val exchange1: String = randomString(4) + "_EX1"
//    val exchange2: String = randomString(4) + "_EX2"
//    val exchange3: String = randomString(4) + "_EX3"
//    val exchange4: String = randomString(4) + "_EX4"
//    val exchange5: String = randomString(4) + "_EX5"
//
//    testHelper.queue.delete(queueName1)
//    testHelper.queue.delete(queueName2)
//
//    private val original = ConfigFactory.load().getConfig("myConfig")
//
//    val bindConfigs: Array[Config] = original.getObjectList("consumers.testing.bindings").asScala.map(_.toConfig).toArray
//    bindConfigs(0) = bindConfigs(0).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
//    bindConfigs(1) = bindConfigs(1).withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange2))
//
//    val config: Config = original
//      .withValue("republishStrategy.exchangeName", ConfigValueFactory.fromAnyRef(exchange5))
//      .withValue("consumers.testing.queueName", ConfigValueFactory.fromAnyRef(queueName1))
//      .withValue("consumers.testing.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
//      .withValue("consumers.testingStreaming.queueName", ConfigValueFactory.fromAnyRef(queueName1))
//      .withValue("consumers.testingStreaming.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
//      .withValue("consumers.testingStreamingWithTimeout.queueName", ConfigValueFactory.fromAnyRef(queueName1))
//      .withValue("consumers.testingStreamingWithTimeout.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
//      .withValue("producers.testing.exchange", ConfigValueFactory.fromAnyRef(exchange1))
//      .withValue("producers.testing2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
//      .withValue("producers.testing3.exchange", ConfigValueFactory.fromAnyRef(exchange4))
//      .withValue("declarations.foo.declareExchange.name", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("declarations.bindExchange1.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange4))
//      .withValue("declarations.bindExchange1.destExchangeName", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("declarations.bindExchange2.sourceExchangeName", ConfigValueFactory.fromAnyRef(exchange4))
//      .withValue("declarations.bindExchange2.destExchangeName", ConfigValueFactory.fromAnyRef(exchange1))
//      .withValue("declarations.declareQueue.name", ConfigValueFactory.fromAnyRef(queueName2))
//      .withValue("declarations.bindQueue.exchangeName", ConfigValueFactory.fromAnyRef(exchange3))
//      .withValue("declarations.bindQueue.queueName", ConfigValueFactory.fromAnyRef(queueName2))
//
//    val ex: ExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
//
//    implicit val sched: Scheduler = Scheduler(Executors.newCachedThreadPool())
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
//          poisoned.incrementAndGet()
//          ()
//        }
//      }
//
//      rabbitConnection
//        .newConsumer("testing", Monitor.noOp(), List(h)) { _: Delivery[Bytes] =>
//          Task {
//            processed.incrementAndGet()
//            DeliveryResult.Republish()
//          }
//        }
//        .withResource { _ =>
//          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
//            for (_ <- 1 to 10) {
//              sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//            }
//
//            eventually(timeout(Span(2, Seconds)), interval(Span(0.25, Seconds))) {
//              assertResult(20)(processed.get())
//              assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
//              assertResult(10)(poisoned.get())
//            }
//          }
//        }
//    }
//  }
//
//  test("PoisonedMessageHandler with timeouting messages") {
//    val c = createConfig()
//    import c._
//
//    val messagesCount = 200
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val poisoned = new AtomicInteger(0)
//      val processed = new AtomicInteger(0)
//
//      val h = PoisonedMessageHandler.withCustomPoisonedAction[Task, Bytes](2) { _ =>
//        Task {
//          poisoned.incrementAndGet()
//          ()
//        }
//      }
//
//      rabbitConnection
//        .newConsumer("testing", Monitor.noOp(), List(h)) {
//          case Delivery.Ok(body, _, _) =>
//            val n = body.toStringUtf8.toInt
//
//            /* This basically means every second message should be timed-out. That will cause it to be republished.
//             * Thx to maxAttempts = 2, this will be done twice in a row, so the resulting numbers are:
//             *
//             *  - processed     = messagesCount * 1.5
//             *  - poisoned      = messagesCount / 2
//             *  - rest in queue = 0
//             *
//             */
//
//            Task { processed.incrementAndGet() } >>
//              sleepIfEven(n, 600.millis) >> // timeout is 500 ms
//              Task.now(DeliveryResult.Ack)
//
//          case _ => fail()
//        }
//        .withResource { _ =>
//          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
//            for (n <- 1 to messagesCount) {
//              sender.send("test", Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
//            }
//
//            eventually(timeout(Span(3, Seconds)), interval(Span(0.25, Seconds))) {
//              println(s"PROCESSED COUNT: ${processed.get()}")
//              assertResult(1.5 * messagesCount)(processed.get())
//              assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
//              assertResult(messagesCount / 2)(poisoned.get())
//            }
//          }
//        }
//    }
//  }
//
//  test("PoisonedMessageHandler streaming") {
//    val c = createConfig()
//    import c._
//
//    val messagesCount = Random.nextInt(10000) + 10000
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val processed = new AtomicInteger(0)
//
//      val pmh = StreamingPoisonedMessageHandler[Task, Bytes](2)
//
//      rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp(), List(pmh)).withResource { cons =>
//        cons.deliveryStream
//          .evalMap {
//            _.handleWith { _ =>
//              Task { processed.incrementAndGet() } >>
//                Task.now(DeliveryResult.Republish())
//            }
//          }
//          .compile
//          .drain
//          .runToFuture
//
//        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to messagesCount) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//          }
//
//          eventually(timeout(Span(20, Seconds)), interval(Span(0.25, Seconds))) {
//            assertResult(2 * messagesCount)(processed.get())
//            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
//          }
//        }
//      }
//    }
//  }
//
//  test("PoisonedMessageHandler streaming custom") {
//    val c = createConfig()
//    import c._
//
//    val messagesCount = Random.nextInt(10000) + 10000
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val poisoned = new AtomicInteger(0)
//      val processed = new AtomicInteger(0)
//
//      val pmh = StreamingPoisonedMessageHandler.withCustomPoisonedAction[Task, Bytes](2) { _ =>
//        Task {
//          logger.debug("Poisoned received!")
//          poisoned.incrementAndGet()
//        }
//      }
//
//      rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp(), List(pmh)).withResource { cons =>
//        cons.deliveryStream
//          .evalMap {
//            _.handleWith { _ =>
//              Task { processed.incrementAndGet() } >>
//                Task.now(DeliveryResult.Republish())
//            }
//          }
//          .compile
//          .drain
//          .runToFuture
//
//        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
//          for (_ <- 1 to messagesCount) {
//            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
//          }
//
//          eventually(timeout(Span(20, Seconds)), interval(Span(0.25, Seconds))) {
//            assertResult(2 * messagesCount)(processed.get())
//            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
//            assertResult(messagesCount)(poisoned.get())
//          }
//        }
//      }
//    }
//  }
//
//  test("PoisonedMessageHandler streaming with timeouting messages") {
//    val c = createConfig()
//    import c._
//
//    val monitor = new TestMonitor
//
//    val messagesCount = 200
//
//    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
//      val poisoned = new AtomicInteger(0)
//      val processed = new AtomicInteger(0)
//
//      val pmh = StreamingPoisonedMessageHandler.withCustomPoisonedAction[Task, Bytes](2) { del =>
//        Task {
//          logger.warn(s"POISONED ${del.asInstanceOf[Delivery.Ok[Bytes]].body.toStringUtf8}")
//          logger.debug("Poisoned received!")
//          poisoned.incrementAndGet()
//        }
//      }
//
//      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithTimeout", monitor, List(pmh)).withResource { cons =>
//        cons.deliveryStream
//          .parEvalMapUnordered(8) {
//            _.handleWith {
//              case Delivery.Ok(body, _, _) =>
//                val n = body.toStringUtf8.toInt
//
//                /* This basically means every second message should be timed-out. That will cause it to be republished.
//                 * Thx to maxAttempts = 2, this will be done twice in a row, so the resulting numbers are:
//                 *
//                 *  - processed     = messagesCount * 1.5
//                 *  - poisoned      = messagesCount / 2
//                 *  - rest in queue = 0
//                 *
//                 */
//
//                Task { processed.incrementAndGet() } >>
//                  sleepIfEven(n, 600.millis) >> // timeout is 500 ms
//                  Task.now(DeliveryResult.Ack)
//
//              case _ => fail()
//            }
//          }
//          .compile
//          .drain
//          .runToFuture
//
//        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
//          for (n <- 1 to messagesCount) {
//            sender.send("test", Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
//          }
//
//          eventually(timeout(Span(30, Seconds)), interval(Span(0.25, Seconds))) {
//            println(s"PROCESSED COUNT: ${processed.get()}")
//            assertResult(1.5 * messagesCount)(processed.get())
//            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
//            assertResult(messagesCount / 2)(poisoned.get())
//          }
//        }
//      }
//    }
//  }
//
//  private def sleepIfEven(n: Int, length: FiniteDuration): Task[Unit] = {
//    if (n % 2 == 0) {
//      Task {
//        logger.warn(s"sleeping _${n}_")
//      } >>
//        Task.sleep(length) >>
//        Task {
//          logger.warn(s"after sleep _${n}_")
//        }
//    } else {
//      Task.unit
//    }
//  }
//}
