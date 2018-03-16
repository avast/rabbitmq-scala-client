package com.avast.clients.rabbitmq.extras

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, MessageProperties}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import monix.execution.Scheduler.Implicits.global
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class PoisonedMessageHandlerTest extends FunSuite with ScalaFutures {

  test("basic") {

    def readAction(d: Delivery): Future[DeliveryResult] = {
      Future.successful(Republish())
    }

    val handler = new PoisonedMessageHandler[Future](5)(readAction)

    def run(properties: MessageProperties): DeliveryResult = {
      handler(Delivery(Bytes.empty(), properties, "")).futureValue
    }

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, _) =>
        run(p) match {
          case Republish(h) => MessageProperties(headers = h)
          case _ => MessageProperties.empty
        }
    }

    // check it increases the header with count
    assertResult(MessageProperties(headers = Map(RepublishCountHeaderName -> 4.asInstanceOf[AnyRef])))(properties)

    // check it will Ack the message on 5th attempt
    assertResult(DeliveryResult.Reject)(run(properties))

  }

  test("pretend lower no. of attempts") {

    def readAction(d: Delivery): Future[DeliveryResult] = {
      Future.successful(Republish())
    }

    val handler = new PoisonedMessageHandler[Future](5)(readAction)

    def run(properties: MessageProperties): DeliveryResult = {
      handler(Delivery(Bytes.empty(), properties, "")).futureValue
    }

    val properties = (1 to 4).foldLeft(MessageProperties.empty) {
      case (p, i) =>
        run(p) match {
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
}
