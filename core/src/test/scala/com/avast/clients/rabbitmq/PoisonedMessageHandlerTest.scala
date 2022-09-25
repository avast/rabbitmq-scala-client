package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.api.DeliveryResult.{DirectlyPoison, Republish}
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaeffectapi.Monitor
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

class PoisonedMessageHandlerTest extends TestBase {

  implicit val dctx: DeliveryContext = TestDeliveryContext.create()

  private val pmhHelper = PoisonedMessageHandlerHelper[Task, PoisonedMessageHandlerTest](Monitor.noOp(), redactPayload = false)

  test("PoisonedMessageHandler.handleResult ignores non-poisoned") {
    val movedCount = new AtomicInteger(0)

    val action = new PoisonedMessageHandlerAction[Task, Bytes] {
      override def handlePoisonedMessage(rawBody: Bytes)(delivery: Delivery[Bytes],
                                                         maxAttempts: Int)(implicit dctx: DeliveryContext): Task[Unit] = Task.delay {
        movedCount.incrementAndGet()
      }
    }

    PoisonedMessageHandler
      .handleResult[Task, Bytes](Delivery.Ok(Bytes.empty(), MessageProperties(), ""),
                                 Bytes.empty(),
                                 MessageId("msg-id"),
                                 1,
                                 pmhHelper,
                                 None,
                                 action)(Republish(countAsPoisoned = false))
      .await

    assertResult(0)(movedCount.get())

    movedCount.set(0)

    PoisonedMessageHandler
      .handleResult[Task, Bytes](Delivery.Ok(Bytes.empty(), MessageProperties(), ""),
                                 Bytes.empty(),
                                 MessageId("msg-id"),
                                 1,
                                 pmhHelper,
                                 None,
                                 action)(Republish())
      .await

    assertResult(1)(movedCount.get())
  }

  test("LoggingPoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = new LoggingPoisonedMessageHandler[Task, Bytes](5, None, pmhHelper)

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

  test("LoggingPoisonedMessageHandler exponential delay") {
    import scala.concurrent.duration._

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val delay = Some(new ExponentialDelay(1.seconds, 1.seconds, 2, 2.seconds))
    val handler = new LoggingPoisonedMessageHandler[Task, Bytes](5, delay, pmhHelper)
    val timeBeforeExecution = Instant.now()
    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(_, h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    val now = Instant.now()
    assert(now.minusSeconds(7).isAfter(timeBeforeExecution) && now.minusSeconds(8).isBefore(timeBeforeExecution))
    // check it increases the header with count
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 4.asInstanceOf[AnyRef])))(properties)

    // check it will Reject the message on 5th attempt
    assertResult(DeliveryResult.Reject)(run(handler, readAction, properties))
  }

  test("LoggingPoisonedMessageHandler direct poisoning") {
    import scala.concurrent.duration._

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(DirectlyPoison)
    }

    val delay = Some(new ExponentialDelay(1.seconds, 1.seconds, 2, 2.seconds))
    val handler = new LoggingPoisonedMessageHandler[Task, Bytes](5, delay, pmhHelper)

    // check it will Reject the message on every attempt
    for (_ <- 1 to Random.nextInt(10) + 10) {
      assertResult(DeliveryResult.Reject)(run(handler, readAction, MessageProperties.empty))
    }
  }

  test("NoOpPoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = new NoOpPoisonedMessageHandler[Task, Bytes](pmhHelper)

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

  test("NoOpPoisonedMessageHandler does nothing") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = new NoOpPoisonedMessageHandler[Task, Bytes](pmhHelper)

    assertResult(DeliveryResult.Ack)(run(handler, _ => Task.now(DeliveryResult.Ack), MessageProperties.empty))
    assertResult(DeliveryResult.Retry)(run(handler, _ => Task.now(DeliveryResult.Retry), MessageProperties.empty))
    assertResult(DeliveryResult.Reject)(run(handler, _ => Task.now(DeliveryResult.Reject), MessageProperties.empty))
    assertResult(DeliveryResult.Republish())(run(handler, _ => Task.now(DeliveryResult.Republish()), MessageProperties.empty))

    // this is the only exception... turn DirectlyPoison to Reject
    assertResult(DeliveryResult.Reject)(run(handler, _ => Task.now(DeliveryResult.DirectlyPoison), MessageProperties.empty))
  }

  test("DeadQueuePoisonedMessageHandler basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val movedCount = new AtomicInteger(0)

    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](5, None, pmhHelper)({ (_, _, _) =>
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

  test("DeadQueuePoisonedMessageHandler direct poisoning") {
    import scala.concurrent.duration._

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(DirectlyPoison)
    }

    val movedCount = new AtomicInteger(0)

    val delay = Some(new ExponentialDelay(1.seconds, 1.seconds, 2, 2.seconds))
    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](5, delay, pmhHelper)({ (_, _, _) =>
      Task.delay(movedCount.incrementAndGet())
    })

    // check it will Reject the message on every attempt
    val cnt = Random.nextInt(10) + 10

    for (_ <- 1 to cnt) {
      assertResult(DeliveryResult.Reject)(run(handler, readAction, MessageProperties.empty))
    }

    assertResult(cnt)(movedCount.get())
  }

  test("DeadQueuePoisonedMessageHandler adds discarded time") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val movedCount = new AtomicInteger(0)

    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](3, None, pmhHelper)({ (d, _, _) =>
      // test it's there and it can be parsed
      assert(Instant.parse(d.properties.headers(DiscardedTimeHeaderName).asInstanceOf[String]).toEpochMilli > 0)

      Task.delay(movedCount.incrementAndGet())
    })

    val properties = (1 to 2).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(_, h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    assertResult(DeliveryResult.Reject)(run(handler, readAction, properties))

    // if the assert above has failed, this won't assert
    assertResult(1)(movedCount.get())
  }

  test("pretend lower no. of attempts") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val movedCount = new AtomicInteger(0)

    val handler = new DeadQueuePoisonedMessageHandler[Task, Bytes](5, None, pmhHelper)({ (_, _, _) =>
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
