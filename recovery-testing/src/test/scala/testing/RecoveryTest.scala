package testing

import cats.effect.Blocker
import cats.effect.concurrent.Ref
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.pureconfig._
import com.avast.clients.rabbitmq.{ConnectionListener, RabbitMQConnection}
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.{Connection, ShutdownSignalException}
import com.spotify.docker.client.DefaultDockerClient
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Random

@Ignore // we don't want to run this test during every build
@RunWith(classOf[JUnitRunner])
class RecoveryTest extends AnyFunSuite with StrictLogging {
  private lazy val config = ConfigFactory.load().getConfig("recoveryTesting")
  private lazy val queueName = config.getString("consumers.consumer.queueName")
  private lazy val blockingExecutor = Executors.newCachedThreadPool()
  private lazy val blocker = Blocker.liftExecutorService(blockingExecutor)

  private lazy val dockerClient = new DefaultDockerClient("unix:///var/run/docker.sock")

  val run: Task[Unit] = {
    val messagesReceived = Ref.unsafe[Task, Set[String]](Set.empty)
    val messagesSent = Ref.unsafe[Task, Set[String]](Set.empty)

    val messagesBatchSize = 200

    val connectionListener = new RecoveryAwareListener

    RabbitMQConnection
      .fromConfig[Task](config, blockingExecutor, connectionListener = Some(connectionListener))
      .use { conn =>
        val consumer = conn.newConsumer[String]("consumer", Monitor.noOp()) {
          case Delivery.Ok(body, props, _) if body == "ahoj" =>
            Task.sleep(150.millis) >>
              messagesReceived.update(_ + props.messageId.getOrElse(sys.error("no MSG ID?"))).as(DeliveryResult.Ack)

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
                // Sends $messagesBatchSize messages, tries to retry in case of failure.
                val sendMessages = fs2.Stream
                  .range[Task](0, messagesBatchSize)
                  .evalMap { _ =>
                    Task {
                      Random.alphanumeric.take(10).mkString
                    }.flatMap { id =>
                      val props = MessageProperties(messageId = Some(id))

                      Task.sleep(100.millis) >>
                        retryExponentially(producer.send(queueName, "ahoj", Some(props)), 2) >>
                        messagesSent.update(_ + id)
                    }
                  }
                  .compile
                  .drain

                // Phase 1: Sends & receives $messagesBatchSize messages. Restart nodes on background.
                val phase1 = sendMessages.start
                  .flatMap { producerFiber =>
                    val containers = dockerClient.listContainers().asScala.toVector

                    def restart(i: Int): Task[Unit] = {
                      val container = containers(i)
                      Task { println(s"#######\n#######\nRestarting ${container.names().asScala.mkString(" ")}\n#######\n#######") } >>
                        blocker.delay[Task, Unit] { dockerClient.restartContainer(container.id(), 0) }
                    }

                    val restartAll = (0 to 2).foldLeft(Task.unit) { case (p, n) => p >> Task.sleep(20.seconds) >> restart(n) }

                    restartAll >>
                      Task.sleep(20.seconds) >>
                      producerFiber.join
                  }

                // Phase 2: Sends & receives $messagesBatchSize messages. To verify everything was recovered successfully.
                val phase2 = sendMessages

                Task(println("Phase 1 START")) >>
                  phase1 >>
                  Task(println("Phase 2 START")) >>
                  phase2 >>
                  Task(println("Wait for consumer to finish")) >>
                  Task.sleep(10.seconds) // let the consumer finish the job
              }
          } >> { // Verify the behavior:
          for {
            sent <- messagesSent.get
            received <- messagesReceived.get
            recStarted <- connectionListener.recoveryStarted.get
            recCompleted <- connectionListener.recoveryCompleted.get
          } yield {
            val inFlight = sent.size - received.size
            println(s"Sent ${sent.size}, received (unique) ${received.size}, in-flight: $inFlight")
            println(s"Recovery: started ${recStarted}x, completed ${recCompleted}X")

            assertResult(sent)(received) // ensure everything was transferred (note: at least once)
            assertResult(messagesBatchSize * 2)(received.size)

            assertResult(recStarted)(recCompleted)
            assert(recStarted > 0) // ensure there was at least one recovery, might be more
          }
        }
      }
      .void
  }

  test("run") {
    run.runSyncUnsafe(5.minutes)
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

class RecoveryAwareListener extends ConnectionListener[Task] {
  val recoveryStarted: Ref[Task, Int] = Ref.unsafe[Task, Int](0)
  val recoveryCompleted: Ref[Task, Int] = Ref.unsafe[Task, Int](0)

  private val logger = Slf4jLogger.getLoggerFromClass[Task](this.getClass)

  override def onCreate(connection: Connection): Task[Unit] = {
    logger.info(s"Connection created: $connection (name ${connection.getClientProvidedName})")
  }

  override def onCreateFailure(failure: Throwable): Task[Unit] = {
    logger.warn(failure)(s"Connection NOT created")
  }

  override def onRecoveryStarted(connection: Connection): Task[Unit] = {
    recoveryStarted.update(_ + 1) >>
      logger.info(s"Connection recovery started: $connection (name ${connection.getClientProvidedName})")
  }

  override def onRecoveryCompleted(connection: Connection): Task[Unit] = {
    recoveryCompleted.update(_ + 1) >>
      logger.info(s"Connection recovery completed: $connection (name ${connection.getClientProvidedName})")
  }

  override def onRecoveryFailure(connection: Connection, failure: Throwable): Task[Unit] = {
    logger.warn(failure)(s"Connection recovery failed: $connection (name ${connection.getClientProvidedName})")
  }

  override def onShutdown(connection: Connection, cause: ShutdownSignalException): Task[Unit] = {
    if (cause.isInitiatedByApplication) {
      logger.info(s"App-initiated connection shutdown: $connection (name ${connection.getClientProvidedName})")
    } else {
      logger.warn(cause)(s"Server-initiated connection shutdown: $connection (name ${connection.getClientProvidedName})")
    }
  }
}
