package testing

import cats.effect.Blocker
import cats.effect.concurrent.Ref
import com.avast.clients.rabbitmq.RabbitMQConnection
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.pureconfig._
import com.avast.metrics.scalaeffectapi.Monitor
import com.spotify.docker.client.DefaultDockerClient
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@RunWith(classOf[JUnitRunner])
class RecoveryTest extends FunSuite with StrictLogging {
  private lazy val config = ConfigFactory.load().getConfig("recoveryTesting")
  private lazy val queueName = config.getString("consumers.consumer.queueName")
  private lazy val blockingExecutor = Executors.newCachedThreadPool()
  private lazy val blocker = Blocker.liftExecutorService(blockingExecutor)

  private lazy val dockerClient = new DefaultDockerClient("unix:///var/run/docker.sock")

  val run: Task[Unit] = {
    val messagesToBeSent = 1000

    val messagesReceived = Ref.unsafe[Task, Int](0)
    val messagesSent = Ref.unsafe[Task, Int](0)

    RabbitMQConnection
      .fromConfig[Task](config, blockingExecutor)
      .use { conn =>
        val consumer = conn.newConsumer[String]("consumer", Monitor.noOp()) {
          case Delivery.Ok(body, _, _) if body == "ahoj" => messagesReceived.update(_ + 1).as(DeliveryResult.Ack)
          case d =>
            Task {
              println(s"Delivery failure! $d")
              DeliveryResult.Reject
            } // what??
        }

        val producer = conn.newProducer[String]("producer", Monitor.noOp())

        consumer
          .use { _ =>
            producer
              .use { producer =>
                fs2.Stream
                  .range[Task](0, messagesToBeSent)
                  .parEvalMap(4) { _ =>
                    Task.sleep(100.millis) >> retryExponentially(producer.send(queueName, "ahoj"), 2) >> messagesSent.update(_ + 1)
                  }
                  .compile
                  .drain
              }
              .start
              .flatMap { producerFiber =>
                val containers = dockerClient.listContainers().asScala.toVector

                def restart(i: Int): Task[Unit] = {
                  val container = containers(i)

                  Task { println(s"#######\n#######\nRestarting ${container.names().asScala.mkString(" ")}\n#######\n#######") } >>
                    blocker.delay[Task, Unit] { dockerClient.restartContainer(container.id(), 0) }
                }

                val restartAll = (0 to 2).foldLeft(Task.unit) { case (p, n) => p >> Task.sleep(10.seconds) >> restart(n) }

                restartAll >> producerFiber.join
              } >> Task(println("Wait for consumer to finish")) >> Task.sleep(10.seconds) // let the consumer finish the job
          } >> {
          for {
            sent <- messagesSent.get
            received <- messagesReceived.get
          } yield {
            val inFlight = sent - received
            println(s"Sent $sent, received $received, in-flight: $inFlight")
            assertResult(0)(inFlight)
          }

        }
      }
      .void
  }

  test("run") {
    run.runSyncUnsafe(2.minutes)
  }

  private def retryExponentially[A](t: Task[A], maxRetries: Int): Task[A] = {
    t.onErrorRestartLoop(maxRetries) {
      case (err, counter, f) =>
        (err, counter) match {
          case (t, n) if n > 0 =>
            val backOffTime = 2 << (maxRetries - counter) // backOffTimes 2, 4 & 8 seconds respectively
            Task(logger.error(s"${t.getClass.getName} encountered - going to do one more attempt after $backOffTime seconds")) >>
              f(counter - 1).delayExecution(backOffTime.seconds)

          case (err, _) => Task.raiseError(err)
        }
    }
  }
}
