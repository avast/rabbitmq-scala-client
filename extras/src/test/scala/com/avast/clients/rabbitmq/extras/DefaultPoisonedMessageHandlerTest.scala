package com.avast.clients.rabbitmq.extras

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures

class DefaultPoisonedMessageHandlerTest extends TestBase with ScalaFutures {

  test("basic") {
    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = PoisonedMessageHandler[Task, Bytes](5)

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(handler, readAction, p) match {
          case Republish(h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    // check it increases the header with count
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 4.asInstanceOf[AnyRef])))(properties)

    // check it will Ack the message on 5th attempt
    assertResult(DeliveryResult.Reject)(run(handler, readAction, properties))

  }

  test("pretend lower no. of attempts") {

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = PoisonedMessageHandler[Task, Bytes](5)

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, i) =>
        run(handler, readAction, p) match {
          case Republish(h) =>
            if (i == 3) {
              MessageProperties(headers = h + (RepublishCountHeaderName -> 1.asInstanceOf[AnyRef]))
            } else {
              MessageProperties(headers = h)
            }
          case _ => MessageProperties.empty
        }
    }

    // attempts no. will be only 2 because programmer said that ;-)
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 2.asInstanceOf[AnyRef])))(properties)
  }

  def run(handler: PoisonedMessageHandler[Task, Bytes],
          readAction: Delivery[Bytes] => Task[DeliveryResult],
          properties: MessageProperties): DeliveryResult = {
    val delivery = Delivery(Bytes.empty(), properties, "")

    handler.adjustDelivery(delivery) >>
      handler.adjustResult(delivery, readAction(delivery))
  }.runSyncUnsafe()
}
