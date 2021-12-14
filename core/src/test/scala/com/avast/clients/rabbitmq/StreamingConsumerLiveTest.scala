package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult._
import com.avast.clients.rabbitmq.api._
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

class StreamingConsumerLiveTest extends TestBase with ScalaFutures {
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
      .withValue("consumers.testingStreaming.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingStreaming.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("consumers.testingStreamingWithTimeout.queueName", ConfigValueFactory.fromAnyRef(queueName1))
      .withValue("consumers.testingStreamingWithTimeout.bindings", ConfigValueFactory.fromIterable(bindConfigs.toSeq.map(_.root()).asJava))
      .withValue("producers.testing.exchange", ConfigValueFactory.fromAnyRef(exchange1))
      .withValue("producers.testing2.exchange", ConfigValueFactory.fromAnyRef(exchange2))
      .withValue("producers.testing3.exchange", ConfigValueFactory.fromAnyRef(exchange4))

    val ex: ExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    implicit val sched: Scheduler = Scheduler(
      Executors.newScheduledThreadPool(4),
      ExecutionContext.fromExecutor(new ForkJoinPool())
    )
  }

  test("streaming consumer") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val count = Random.nextInt(50000) + 50000 // random 50 - 100k messages

      logger.info(s"Sending $count messages")

      val latch = new CountDownLatch(count + 10000) // explanation below

      val d = new AtomicInteger(0)

      rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp()).withResource { cons =>
        val stream = cons.deliveryStream
          .parEvalMapUnordered(50) {
            _.handleWith { del =>
              Task.delay(d.incrementAndGet()).flatMap { n =>
                Task {
                  latch.countDown()

                  if (n <= (count - 10000) || n > count) Ack
                  else {
                    if (n <= (count - 5000)) Retry else Republish()
                  }

                  // ^ example: 100000 messages in total => 6500 * Ack, 5000 * Retry, 5000 * Republish => processing 110000 (== +10000) messages in total
                }
              }
            }
          }

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to count) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          // it takes some time before the stats appear... :-|
          eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
            assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
          }

          sched.execute(() => stream.compile.drain.runSyncUnsafe()) // run the stream

          eventually(timeout(Span(4, Minutes)), interval(Span(1, Seconds))) {
            println("D: " + d.get())
            assertResult(count + 10000)(d.get())
            assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
            assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
          }
        }
      }
    }
  }

  test("streaming consumers to single queue") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val count = Random.nextInt(10000) + 10000 // random 10k - 20k messages

      logger.info(s"Sending $count messages")

      val latch = new CountDownLatch(count)

      def toStream(cons1: RabbitMQStreamingConsumer[Task, Bytes], d: AtomicInteger): fs2.Stream[Task, Unit] = {
        cons1.deliveryStream
          .parEvalMapUnordered(20) {
            _.handleWith { _ =>
              Task.delay(d.incrementAndGet()).flatMap { n =>
                Task.sleep((if (n % 500 == 0) Random.nextInt(100) else 0).millis) >> // random slowdown 0-100 ms for every 500th message
                  Task {
                    latch.countDown()

                    Ack
                  }
              }
            }
          }
      }

      rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp()).withResource { cons1 =>
        rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp()).withResource { cons2 =>
          val d1 = new AtomicInteger(0)
          val d2 = new AtomicInteger(0)

          val stream1 = toStream(cons1, d1)
          val stream2 = toStream(cons2, d2)

          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
            for (_ <- 1 to count) {
              sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
            }

            // it takes some time before the stats appear... :-|
            eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
              assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
            }

            ex.execute(() => stream1.compile.drain.runSyncUnsafe()) // run the stream
            ex.execute(() => stream2.compile.drain.runSyncUnsafe()) // run the stream

            eventually(timeout(Span(5, Minutes)), interval(Span(5, Seconds))) {
              val inQueue = testHelper.queue.getMessagesCount(queueName1)
              println(s"D: ${d1.get}/${d2.get()}, IN QUEUE $inQueue")
              assertResult(count)(d1.get() + d2.get())
              assert(d1.get() > 0)
              assert(d2.get() > 0)
              println("LATCH: " + latch.getCount)
              assertResult(true)(latch.await(1000, TimeUnit.MILLISECONDS))
              assertResult(0)(inQueue)
            }
          }
        }
      }
    }
  }

  test("streaming consumer stream doesn't fail with failed result") {
    for (_ <- 1 to 5) {
      val c = createConfig()
      import c._

      RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
        val count = Random.nextInt(5000) + 5000 // random 5k - 10k messages

        val nth = 150

        logger.info(s"Sending $count messages")

        val d = new AtomicInteger(0)

        rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp()).withResource { cons =>
          def stream: fs2.Stream[Task, Unit] =
            cons.deliveryStream
              .evalMap {
                _.handleWith { _ =>
                  Task
                    .delay(d.incrementAndGet())
                    .flatMap { n =>
                      if (n % nth != 0) Task.now(Ack)
                      else {
                        Task.raiseError(new RuntimeException(s"My failure $n"))
                      }
                    // ^^ cause failure for every nth message
                    }
                }
              }

          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
            for (_ <- 1 to count) {
              sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
            }

            // it takes some time before the stats appear... :-|
            eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
              assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
            }

            ex.execute(() => stream.compile.drain.runSyncUnsafe()) // run the stream

            eventually(timeout(Span(5, Minutes)), interval(Span(1, Seconds))) {
              println("D: " + d.get())
              assert(d.get() > count) // can't say exact number, number of redeliveries is unpredictable
              assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
            }
          }
        }
      }
    }
  }

  test("streaming consumer stream doesn't fail with thrown exception") {
    for (_ <- 1 to 5) {
      val c = createConfig()
      import c._

      RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
        val count = Random.nextInt(5000) + 5000 // random 5k - 10k messages

        val nth = 150

        logger.info(s"Sending $count messages")

        val d = new AtomicInteger(0)

        rabbitConnection.newStreamingConsumer[Bytes]("testingStreaming", Monitor.noOp()).withResource { cons =>
          def stream: fs2.Stream[Task, Unit] =
            cons.deliveryStream
              .evalMap {
                _.handleWith { _ =>
                  Task
                    .delay(d.incrementAndGet())
                    .flatMap { n =>
                      if (n % nth != 0) Task.now(Ack)
                      else {
                        throw new RuntimeException(s"My failure $n")
                      }
                    // ^^ cause failure for every nth message
                    }
                }
              }

          rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
            for (_ <- 1 to count) {
              sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
            }

            // it takes some time before the stats appear... :-|
            eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
              assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
            }

            ex.execute(() => stream.compile.drain.runSyncUnsafe()) // run the stream

            eventually(timeout(Span(5, Minutes)), interval(Span(1, Seconds))) {
              println("D: " + d.get())
              assert(d.get() > count) // can't say exact number, number of redeliveries is unpredictable
              assertResult(0)(testHelper.queue.getMessagesCount(queueName1))
            }
          }
        }
      }
    }
  }

  test("streaming consumer timeouts") {
    val c = createConfig()
    import c._

    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      val count = 100

      logger.info(s"Sending $count messages")

      val d = new AtomicInteger(0)

      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithTimeout", Monitor.noOp()).withResource { cons =>
        val stream = cons.deliveryStream
          .mapAsyncUnordered(50) {
            _.handleWith { _ =>
              Task.delay(d.incrementAndGet()) >>
                Task
                  .now(Ack)
                  .delayExecution(800.millis)
            }
          }

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to count) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }

          // it takes some time before the stats appear... :-|
          eventually(timeout(Span(5, Seconds)), interval(Span(0.5, Seconds))) {
            assertResult(count)(testHelper.queue.getPublishedCount(queueName1))
          }

          ex.execute(() => stream.compile.drain.runSyncUnsafe()) // run the stream

          eventually(timeout(Span(30, Seconds)), interval(Span(1, Seconds))) {
            println("D: " + d.get())
            assert(d.get() > count + 200) // more than sent messages
            assert(testHelper.exchange.getPublishedCount(exchange5) > 0)
          }
        }
      }
    }
  }

  test("can be closed properly") {
    val c = createConfig()
    import c._

    // single stream
    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithTimeout", Monitor.noOp()).withResource { cons =>
        val stream = cons.deliveryStream
          .evalMap {
            _.handleWith { _ =>
              Task.now(Ack)
            }
          }

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to 50) sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
        }

        stream.take(1).compile.drain.runSyncUnsafe()
      }
    }

    // two streams
    RabbitMQConnection.fromConfig[Task](config, ex).withResource { rabbitConnection =>
      rabbitConnection.newStreamingConsumer[Bytes]("testingStreamingWithTimeout", Monitor.noOp()).withResource { cons =>
        def createStream(): fs2.Stream[Task, Unit] =
          cons.deliveryStream
            .evalMap {
              _.handleWith { _ =>
                Task.now(Ack)
              }
            }

        rabbitConnection.newProducer[Bytes]("testing", Monitor.noOp()).withResource { sender =>
          for (_ <- 1 to 50) {
            sender.send("test", Bytes.copyFromUtf8(Random.nextString(10))).await
          }
        }

        val stream1 = createStream().take(1).compile.drain.start
        val stream2 = createStream().take(1).compile.drain.start

        Task.map2(stream1, stream2)((a, b) => a.join >> b.join).flatten.await
      }
    }

    // ok
  }
}
