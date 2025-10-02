package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.test.FileSource
import org.junit.runner.RunWith
import org.scalatest.flatspec.FlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import scalapb.json4s.Printer

@RunWith(classOf[JUnitRunner])
class ScalaPBConvertersTest extends FlatSpec with Matchers {
  private val event = FileSource("fileId", "source")

  it must "deserialize binary event" in {
    val target = ScalaPBAsBinaryDeliveryConverter.derive[FileSource]()
    val Right(actual) = target.convert(Bytes.copyFrom(event.toByteArray))
    actual shouldBe event
  }

  it must "deserialize json event" in {
    val target = ScalaPBAsJsonDeliveryConverter.derive[FileSource]()
    val Right(actual) = target.convert(Bytes.copyFromUtf8(new Printer().print(event)))
    actual shouldBe event
  }

  it must "serialize binary event" in {
    val target = ScalaPBAsBinaryProductConverter.derive[FileSource]()
    val Right(actual) = target.convert(event)
    actual.toByteArray shouldBe event.toByteArray
  }

  it must "serialize json event" in {
    val target = ScalaPBAsJsonProductConverter.derive[FileSource]()
    val Right(actual) = target.convert(event)
    actual shouldBe Bytes.copyFromUtf8(new Printer().print(event))
  }
}
