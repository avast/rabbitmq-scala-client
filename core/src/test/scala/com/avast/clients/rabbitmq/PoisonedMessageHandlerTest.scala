package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import java.util.concurrent.atomic.AtomicInteger

class PoisonedMessageHandlerTest extends TestBase {

  implicit val cid: CorrelationId = CorrelationId("corr-id")

  private val ilogger = ImplicitContextLogger.createLogger[Task, PoisonedMessageHandlerTest]

  test("PoisonedMessageHandler.handleResult ignores non-poisoned") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish(isPoisoned = false))
    }

    val movedCount = new AtomicInteger(0)

    PoisonedMessageHandler
      .handleResult[Task, Bytes](Delivery.Ok(Bytes.empty(), MessageProperties(), ""), MessageId("msg-id"), 1, ilogger, (_, _) => {
        Task.delay { movedCount.incrementAndGet() }
      })(Republish(isPoisoned = false))
      .await

    assertResult(0)(movedCount.get())

    movedCount.set(0)

    PoisonedMessageHandler
      .handleResult[Task, Bytes](Delivery.Ok(Bytes.empty(), MessageProperties(), ""), MessageId("msg-id"), 1, ilogger, (_, _) => {
        Task.delay { movedCount.incrementAndGet() }
      })(Republish())
      .await

    assertResult(1)(movedCount.get())
  }

  test("LoggingPoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = new LoggingPoisonedMessageHandler[Task, Bytes](5)

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(_, h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    // check it increases the header with count
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 4.asInstanceOf[AnyRef])))(properties)

    // check it will Reject the message on 5th attempt
    assertResult(DeliveryResult.Reject)(run(handler, readAction, properties))
  }

  test("NoOpPoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = new NoOpPoisonedMessageHandler[Task, Bytes]

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(_, h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    // check it increases the header with count
    assertResult(MessageProperties(headers = Map.empty))(properties)
  }

  test("DeadQueuePoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val movedCount = new AtomicInteger(0)

    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](5)({ (_, _, _) =>
      Task.delay(movedCount.incrementAndGet())
    })

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(_, h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    // check it increases the header with count
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 4.asInstanceOf[AnyRef])))(properties)

    // check it will Reject the message on 5th attempt
    assertResult(DeliveryResult.Reject)(run(handler, readAction, properties))

    assertResult(1)(movedCount.get())
  }

  test("pretend lower no. of attempts") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val movedCount = new AtomicInteger(0)

    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](5)({ (_, _, _) =>
      Task.delay(movedCount.incrementAndGet())
    })

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, i) =>
        run(handler, readAction, p) match {
          case Republish(_, h) =>
            if (i == 3) {
              MessageProperties(headers = h + (RepublishCountHeaderName -> 1.asInstanceOf[AnyRef]))
            } else {
              MessageProperties(headers = h)
            }

          case _ => fail("unreachable")
        }
    }

    // attempts no. will be only 2 because programmer said that ;-)
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 2.asInstanceOf[AnyRef])))(properties)

    assertResult(0)(movedCount.get())
  }

  def run(handler: PoisonedMessageHandler[Task, Bytes],
          readAction: Delivery[Bytes] => Task[DeliveryResult],
          properties: MessageProperties): DeliveryResult = {
    val delivery = Delivery(Bytes.empty(), properties, "")

    readAction(delivery).flatMap {
      handler.interceptResult(delivery, MessageId("msg-id"), Bytes.empty())
    }
  }.runSyncUnsafe()
}
