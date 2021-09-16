package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery, DeliveryResult, MessageProperties}
import com.avast.clients.rabbitmq.extras.format._
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class MultiFormatConsumerTest extends TestBase with ScalaFutures {

  val StringDeliveryConverter: CheckedDeliveryConverter[String] = new CheckedDeliveryConverter[String] {
    override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.contains("text/plain")

    override def convert(b: Bytes): Either[ConversionException, String] = Right(b.toStringUtf8)
  }

  private implicit val c: Configuration = Configuration.default.withSnakeCaseMemberNames
  private implicit val d: Decoder[Bytes] = Decoder.decodeString.map(ByteUtils.hexToBytesImmutable)

  case class FileSource(fileId: Bytes, source: String)

  case class NewFileSourceAdded(fileSources: Seq[FileSource])

  test("basic") {
    val consumer = MultiFormatConsumer.forType[Future, String](StringDeliveryConverter) {
      case d: Delivery.Ok[String] =>
        assertResult("abc321")(d.body)
        Future.successful(DeliveryResult.Ack)

      case _ => fail()
    }

    val delivery = Delivery(
      body = Bytes.copyFromUtf8("abc321"),
      properties = MessageProperties(contentType = Some("text/plain")),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Ack)(result)
  }

  test("non-supported content-type") {
    val consumer = MultiFormatConsumer.forType[Future, String](StringDeliveryConverter) {
      case _: Delivery.Ok[String] =>
        Future.successful(DeliveryResult.Ack)
      case _ =>
        Future.successful(DeliveryResult.Reject)
    }

    val delivery = Delivery(
      body = Bytes.copyFromUtf8("abc321"),
      properties = MessageProperties(contentType = Some("text/javascript")),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Reject)(result)
  }

  test("json") {
    val consumer = MultiFormatConsumer.forType[Future, NewFileSourceAdded](JsonDeliveryConverter.derive()) {
      case d: Delivery.Ok[NewFileSourceAdded] =>
        assertResult(
          NewFileSourceAdded(
            Seq(
              FileSource(Bytes.copyFromUtf8("abc"), "theSource"),
              FileSource(Bytes.copyFromUtf8("def"), "theSource")
            )))(d.body)

        Future.successful(DeliveryResult.Ack)

      case _ => Future.successful(DeliveryResult.Reject)
    }

    val delivery = Delivery(
      body = Bytes.copyFromUtf8(s"""
                                   | { "file_sources": [
                                   |   { "file_id": "${ByteUtils.bytesToHex("abc".getBytes)}", "source": "theSource" },
                                   |   { "file_id": "${ByteUtils.bytesToHex("def".getBytes)}", "source": "theSource" }
                                   | ]}
        """.stripMargin),
      properties = MessageProperties(contentType = Some("application/json")),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Ack)(result)
  }

}
