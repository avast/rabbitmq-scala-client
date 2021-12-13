package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.pureconfig._
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

class PoisonedMessageHandlerLiveTest extends TestBase with ScalaFutures {

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
    val exchange5: String = randomString(4) + "_EX5"

    val initialRoutingKey: String = "test_" + randomString(4)
    val deadRoutingKey: String = "dead_" + randomString(4)

    testHelper.queue.delete(queueName1)
    testHelper.queue.delete(queueName2)

    private val original = ConfigFactory.load().getConfig("myConfig")

    val bindConfigs: Array[Config] =
      original.getObjectList("consumers.testingWithPoisonedMessageHandler.bindings").asScala.map(_.toConfig).toArray
    bindConfigs(0) = bindConfigs(0)
      .withValue("exchange.name", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("routingKeys", ConfigValueFactory.fromIterable(Seq(initialRoutingKey).asJava))

    // @formatter:off
    val config: Config = original
      .withValue("republishStrategy.exchangeName", ConfigValueFactory.fromAnyRef(exchange5))
      .withValue("consumers.testingWithPoisonedMessageHandler.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.exchange", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("consumers.testingWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.routingKey", ConfigValueFactory.fromAnyRef(deadRoutingKey))
      .withValue("consumers.testingWithPoisonedMessageHandler.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("consumers.testingStreamingWithPoisonedMessageHandler.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingStreamingWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.exchange", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("consumers.testingStreamingWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.routingKey", ConfigValueFactory.fromAnyRef(deadRoutingKey))
      .withValue("consumers.testingStreamingWithPoisonedMessageHandler.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("consumers.testingPullWithPoisonedMessageHandler.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingPullWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.exchange", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("consumers.testingPullWithPoisonedMessageHandler.poisonedMessageHandling.deadQueueProducer.routingKey", ConfigValueFactory.fromAnyRef(deadRoutingKey))
      .withValue("consumers.testingPullWithPoisonedMessageHandler.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("producers.testing.exchange", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("declarations.declareQueue.name", ConfigValueFactory.fromAnyRef(queueName2))
      .withValue("declarations.bindQueue.exchangeName", ConfigValueFactory.fromAnyRef(exchange3))
      .withValue("declarations.bindQueue.queueName", ConfigValueFactory.fromAnyRef(queueName2))
      .withValue("declarations.bindQueue.routingKeys", ConfigValueFactory.fromIterable(Seq(initialRoutingKey, deadRoutingKey).asJava))
    // @formatter:on

    val ex: ExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    implicit val sched: Scheduler = Scheduler(
      Executors.newScheduledThreadPool(4),
      ExecutionContext.fromExecutor(new ForkJoinPool())
    )
  }

  /*
  Dead queue mapping:

   -- > EXCHANGE3 --(test)--> QUEUE2
   */

  test("PoisonedMessageHandler") {
    val c = createConfig()
    import c._

    val messagesCount = Random.nextInt(5000) + 5000 // 5-10k

    println(s"Sending $messagesCount messages!")

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      rabbitConnection
        .newConsumer("testingWithPoisonedMessageHandler", Monitor.noOp()) { _: Delivery[Bytes] =>
          Task {
            processed.incrementAndGet()
            DeliveryResult.Republish()
          }
        }
        .withResource { _ =>
          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
            // this need to be done _after_ the producer is created (it declares the exchange) but _before_ it starts send messages (so they are not lost)
            rabbitConnection.declareQueue("declareQueue").await
            rabbitConnection.bindQueue("bindQueue").await

            for (n <- 1 to messagesCount) {
              sender.send(initialRoutingKey, Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
            }

            eventually(timeout(Span(90, Seconds)), interval(Span(1, Seconds))) {
              println(s"PROCESSED COUNT: ${processed.get()}")
              assertResult(messagesCount * 2)(processed.get())
              assertResult(0)(testHelper.queue.getMessagesCount(queueName1)) // original dest. queue
              assertResult(messagesCount)(testHelper.queue.getMessagesCount(queueName2)) // dead queue
            }
          }
        }
    }
  }

  test("PoisonedMessageHandler with timeouting messages") {
    val c = createConfig()
    import c._

    val messagesCount = (Random.nextInt(1000) + 1000) * 2 // 2-4k, even

    println(s"Sending $messagesCount messages!")

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      rabbitConnection
        .newConsumer[Bytes]("testingWithPoisonedMessageHandler", Monitor.noOp()) {
          case Delivery.Ok(body, _, _) =>
            val n = body.toStringUtf8.toInt

            /* This basically means every second message should be timed-out. That will cause it to be republished.
             * Thx to maxAttempts = 2, this will be done twice in a row, so the resulting numbers are:
             *
             *  - processed     = messagesCount * 1.5 (half of messages is "processed" twice)
             *  - poisoned      = messagesCount / 2 (half of messages is "thrown away")
             *  - rest in queue = 0
             *
             */

            Task {
              processed.incrementAndGet()
            } >>
              sleepIfEven(n, 800.millis) >> // timeout is 500 ms, this need to be quite much longer to be deterministic
              Task.now(DeliveryResult.Ack)

          case _ => fail()
        }
        .withResource { _ =>
          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
            // this need to be done _after_ the producer is created (it declares the exchange) but _before_ it starts send messages (so they are not lost)
            rabbitConnection.declareQueue("declareQueue").await
            rabbitConnection.bindQueue("bindQueue").await

            for (n <- 1 to messagesCount) {
              sender.send(initialRoutingKey, Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
            }

            eventually(timeout(Span(90, Seconds)), interval(Span(1, Seconds))) {
              println(s"PROCESSED COUNT: ${processed.get()}")
              assertResult(1.5 * messagesCount)(processed.get())
              assertResult(0)(testHelper.queue.getMessagesCount(queueName1)) // original dest. queue
              assertResult(messagesCount / 2)(testHelper.queue.getMessagesCount(queueName2)) // dead queue
            }
          }
        }
    }
  }

  test("PoisonedMessageHandler pull") {
    val c = createConfig()
    import c._

    val messagesCount = Random.nextInt(2000) + 2000 // only 2-4k, this consumer is just slow

    println(s"Sending $messagesCount messages!")

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      rabbitConnection.newPullConsumer[Bytes]("testingPullWithPoisonedMessageHandler", Monitor.noOp()).withResource { cons =>
        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          // this need to be done _after_ the producer is created (it declares the exchange) but _before_ it starts send messages (so they are not lost)
          rabbitConnection.declareQueue("declareQueue").await
          rabbitConnection.bindQueue("bindQueue").await

          for (n <- 1 to messagesCount) {
            sender.send(initialRoutingKey, Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
          }

          // run async:
          ex.execute(() => {
            while (true) {
              val PullResult.Ok(dwh) = cons.pull().await
              processed.incrementAndGet()
              dwh.handle(DeliveryResult.Republish()).await
            }
          })

          eventually(timeout(Span(90, Seconds)), interval(Span(1, Seconds))) {
            println(s"PROCESSED COUNT: ${processed.get()}")
            assertResult(2 * messagesCount)(processed.get())
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1)) // original dest. queue
            assertResult(messagesCount)(testHelper.queue.getMessagesCount(queueName2)) // dead queue
          }
        }
      }
    }
  }

  test("PoisonedMessageHandler streaming") {
    val c = createConfig()
    import c._

    val messagesCount = Random.nextInt(5000) + 5000 // 5-10k

    println(s"Sending $messagesCount messages!")

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithPoisonedMessageHandler", Monitor.noOp()).withResource { cons =>
        cons.deliveryStream
          .evalMap {
            _.handleWith { _ =>
              Task {
                processed.incrementAndGet()
                DeliveryResult.Republish()
              }
            }
          }
          .compile
          .drain
          .runToFuture

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          // this need to be done _after_ the producer is created (it declares the exchange) but _before_ it starts send messages (so they are not lost)
          rabbitConnection.declareQueue("declareQueue").await
          rabbitConnection.bindQueue("bindQueue").await

          for (n <- 1 to messagesCount) {
            sender.send(initialRoutingKey, Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
          }

          eventually(timeout(Span(30, Seconds)), interval(Span(1, Seconds))) {
            println(s"PROCESSED COUNT: ${processed.get()}")
            // we can't assert the `processed` here - some deliveries may have been cancelled before they were even executed
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1)) // original dest. queue
            assertResult(messagesCount)(testHelper.queue.getMessagesCount(queueName2)) // dead queue
          }
        }
      }
    }
  }

  test("PoisonedMessageHandler streaming with timeouting messages") {
    val c = createConfig()
    import c._

    val monitor = new TestMonitor

    val messagesCount = (Random.nextInt(2000) + 2000) * 2 // 4-8k, even

    println(s"Sending $messagesCount messages!")

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val processed = new AtomicInteger(0)

      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithPoisonedMessageHandler", monitor).withResource { cons =>
        cons.deliveryStream
          .parEvalMapUnordered(200) {
            _.handleWith {
              case Delivery.Ok(body, _, _) =>
                val n = body.toStringUtf8.toInt

                /* This basically means every second message should be timed-out. That will cause it to be republished.
                 * Thx to maxAttempts = 2, this will be done twice in a row, so the resulting numbers are:
                 *
                 *  - poisoned      = messagesCount / 2 (half of messages is "thrown away")
                 *  - rest in queue = 0
                 *
                 */

                Task {
                  processed.incrementAndGet()
                } >>
                  sleepIfEven(n, 800.millis) >> // timeout is 500 ms, this need to be quite much longer to be deterministic
                  Task.now(DeliveryResult.Ack)

              case _ => fail()
            }
          }
          .compile
          .drain
          .runToFuture

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          // this need to be done _after_ the producer is created (it declares the exchange) but _before_ it starts send messages (so they are not lost)
          rabbitConnection.declareQueue("declareQueue").await
          rabbitConnection.bindQueue("bindQueue").await

          for (n <- 1 to messagesCount) {
            sender.send(initialRoutingKey, Bytes.copyFromUtf8(n.toString), Some(MessageProperties(messageId = Some(s"msg_${n}_")))).await
          }

          eventually(timeout(Span(90, Seconds)), interval(Span(1, Seconds))) {
            println(s"PROCESSED COUNT: ${processed.get()}")
            // we can't assert the `processed` here - some deliveries may have been cancelled before they were even executed
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
            assertResult(messagesCount / 2)(testHelper.queue.getMessagesCount(queueName2)) // dead queue
          }
        }
      }
    }
  }

  private def sleepIfEven(n: Int, length: FiniteDuration): Task[Unit] = {
    if (n % 2 == 0) {
      Task.sleep(length)
    } else {
      Task.unit
    }
  }
}
