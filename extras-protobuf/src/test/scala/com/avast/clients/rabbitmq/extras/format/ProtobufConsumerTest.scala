package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.test.ExampleEvents
import com.google.protobuf.util.JsonFormat
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProtobufConsumerTest extends FlatSpec with Matchers {
  private val event = ExampleEvents.FileSource.newBuilder().setFileId("fileId").setSource("source").build()

  it must "consume JSON and binary events" in {
    val consumer = ProtobufConsumer.create[Task, ExampleEvents.FileSource] {
      case Delivery.Ok(actual, _, _) =>
        actual shouldBe event
        Task.pure(DeliveryResult.Ack)

      case _ => fail("malformed")
    }
    consumer(Delivery.Ok(Bytes.copyFrom(event.toByteArray), MessageProperties.empty.copy(contentType = Some("application/protobuf")), ""))
      .runSyncUnsafe() shouldBe DeliveryResult.Ack
    consumer(
      Delivery.Ok(Bytes.copyFromUtf8(JsonFormat.printer().print(event)),
                  MessageProperties.empty.copy(contentType = Some("application/json")),
                  "")).runSyncUnsafe() shouldBe DeliveryResult.Ack
  }
}
