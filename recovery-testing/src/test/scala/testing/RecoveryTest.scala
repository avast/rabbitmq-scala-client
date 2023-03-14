package testing

import cats.effect.Blocker
import cats.effect.concurrent.Ref
import com.avast.clients.rabbitmq.RabbitMQConnection
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.pureconfig._
import com.avast.metrics.scalaeffectapi.Monitor
import com.spotify.docker.client.DefaultDockerClient
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@RunWith(classOf[JUnitRunner])
class RecoveryTest extends FunSuite {
  private lazy val config = ConfigFactory.load().getConfig("recoveryTesting")
  private lazy val queueName = config.getString("consumers.consumer.queueName")
  private lazy val blockingExecutor = Executors.newCachedThreadPool()
  private lazy val blocker = Blocker.liftExecutorService(blockingExecutor)

  private lazy val dockerClient = new DefaultDockerClient("unix:///var/run/docker.sock")

  val run: Task[Unit] = {
    val messagesInFlight = Ref.unsafe[Task, Int](0)

    RabbitMQConnection
      .fromConfig[Task](config, blockingExecutor)
      .use { conn =>
        val consumer = conn.newConsumer[String]("consumer", Monitor.noOp()) {
          case Delivery.Ok(body, _, _) if body == "ahoj" => messagesInFlight.update(_ - 1).as(DeliveryResult.Ack)
          case _ => Task.now(DeliveryResult.Reject) // what??
        }

        val producer = conn.newProducer[String]("producer", Monitor.noOp())

        consumer
          .use { _ =>
            producer
              .use { producer =>
                fs2.Stream
                  .repeatEval[Task, Unit] {
                    Task.sleep(100.millis) >> producer.send(queueName, "ahoj") >> messagesInFlight.update(_ + 1)
                  }
                  .compile
                  .drain
              }
              .start
              .flatMap { producerFiber =>
                val containers = dockerClient.listContainers().asScala.toVector

                def restart(i: Int): Task[Unit] = {
                  val container = containers(i)

                  Task { println(s"Restarting ${container.names().asScala.mkString(" ")} ") } >>
                    blocker.delay[Task, Unit] { dockerClient.restartContainer(container.id()) }
                }

                val restartAll = (0 to 2).foldLeft(Task.unit) { case (p, n) => p >> restart(n) }

                restartAll >>
                  Task.sleep(10.seconds) >>
                  producerFiber.cancel
              } >> Task(println("Wait for consumer to finish")) >> Task.sleep(10.seconds) // let the consumer finish the job
          } >> messagesInFlight.get.map { m =>
          println(s"In-flight messages: $m")
          assertResult(0)(m)
        }
      }
      .void
  }

  test("run") {
    run.runSyncUnsafe(2.minutes)
  }
}
