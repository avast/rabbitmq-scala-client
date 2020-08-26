package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.test.ExampleEvents
import com.google.protobuf.util.JsonFormat
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProtobufConvertersTest extends FlatSpec with Matchers {
  private val event = ExampleEvents.FileSource.newBuilder().setFileId("fileId").setSource("source").build()

  it must "deserialize binary event" in {
    val target = ProtobufAsBinaryDeliveryConverter.derive[ExampleEvents.FileSource]()
    val Right(actual) = target.convert(Bytes.copyFrom(event.toByteArray))
    actual shouldBe event
  }

  it must "deserialize json event" in {
    val target = ProtobufAsJsonDeliveryConverter.derive[ExampleEvents.FileSource]()
    val Right(actual) = target.convert(Bytes.copyFromUtf8(JsonFormat.printer().print(event)))
    actual shouldBe event
  }

  it must "serialize binary event" in {
    val target = ProtobufAsBinaryProductConverter.derive[ExampleEvents.FileSource]()
    val Right(actual) = target.convert(event)
    actual.toByteArray shouldBe event.toByteArray
  }

  it must "serialize json event" in {
    val target = ProtobufAsJsonProductConverter.derive[ExampleEvents.FileSource]()
    val Right(actual) = target.convert(event)
    actual shouldBe Bytes.copyFromUtf8(JsonFormat.printer().print(event))
  }
}
