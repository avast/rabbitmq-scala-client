package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.test.FileSource
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner
import scalapb.json4s.Printer

@RunWith(classOf[JUnitRunner])
class ScalaPBConsumerTest extends FlatSpec with Matchers {
  private val event = FileSource("fileId", "source")

  it must "consume JSON and binary events" in {
    val consumer = ScalaPBConsumer.create[Task, FileSource] {
      case Delivery.Ok(actual, _, _) =>
        actual shouldBe event
        Task.pure(DeliveryResult.Ack)

      case _ => fail("malformed")
    }
    consumer(Delivery.Ok(Bytes.copyFrom(event.toByteArray), MessageProperties.empty.copy(contentType = Some("application/protobuf")), ""))
      .runSyncUnsafe() shouldBe DeliveryResult.Ack
    consumer(
      Delivery.Ok(Bytes.copyFromUtf8(new Printer().print(event)), MessageProperties.empty.copy(contentType = Some("application/json")), ""))
      .runSyncUnsafe() shouldBe DeliveryResult.Ack
  }
}
