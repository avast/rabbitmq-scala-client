package testing

import com.avast.clients.rabbitmq.RabbitMQConnection
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.pureconfig._
import com.avast.metrics.scalaeffectapi.Monitor
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class RecoveryTest extends FunSuite {
  private lazy val config = ConfigFactory.load().getConfig("recoveryTesting")
  private lazy val queueName = config.getString("consumers.consumer.queueName")

  val run: Task[Unit] = {
    RabbitMQConnection
      .fromConfig[Task](config, Executors.newCachedThreadPool())
      .use { conn =>
        val consumer = conn.newConsumer[String]("consumer", Monitor.noOp()) {
          case Delivery.Ok(body, _, _) => Task.now(DeliveryResult.Ack)
          case _ => Task.now(DeliveryResult.Reject)
        }

        val producer = conn.newProducer[String]("producer", Monitor.noOp())

        consumer.use { _ =>
          producer.use { producer =>
            producer.send(queueName, "ahoj")
          }
        }
      }
      .void
  }

  test("run") {
    run.runSyncUnsafe(1.minute)
  }
}
