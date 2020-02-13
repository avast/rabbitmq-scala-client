package com.avast.clients.rabbitmq.extras

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.Republish
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures

class StreamingPoisonedMessageHandlerTest extends TestBase with ScalaFutures {

  private def streamedDelivery[A](body: A, properties: MessageProperties) = new StreamedDelivery[Task, A] {
    override val delivery: Delivery[A] = Delivery(body, properties, "")

    override def handle(result: DeliveryResult): Task[StreamedResult] = Task.now {
      this.result = Some(result)
      StreamedResult
    }

    //noinspection ScalaStyle
    var result: Option[DeliveryResult] = None
  }

  test("basic") {

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = StreamingPoisonedMessageHandler[Task, Bytes](5)(readAction)

    def run(properties: MessageProperties): DeliveryResult = {
      //noinspection ScalaStyle
      val delivery: StreamedDelivery[Task, Bytes] { var result: Option[DeliveryResult] } = streamedDelivery(Bytes.empty(), properties)

      handler(fs2.Stream.eval(Task.now(delivery))).compile.last.runSyncUnsafe().getOrElse(fail())
      delivery.result.getOrElse(fail())
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

    def readAction(d: Delivery[Bytes]): Task[DeliveryResult] = {
      Task.now(Republish())
    }

    val handler = StreamingPoisonedMessageHandler[Task, Bytes](5)(readAction)

    def run(properties: MessageProperties): DeliveryResult = {
      //noinspection ScalaStyle
      val delivery: StreamedDelivery[Task, Bytes] { var result: Option[DeliveryResult] } = streamedDelivery(Bytes.empty(), properties)

      handler(fs2.Stream.eval(Task.now(delivery))).compile.last.runSyncUnsafe().getOrElse(fail())
      delivery.result.getOrElse(fail())
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
