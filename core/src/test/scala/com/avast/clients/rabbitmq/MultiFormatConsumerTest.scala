package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.bytes.gpb.ByteStringBytes
import com.avast.cactus.bytes._
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, MessageProperties}
import com.avast.clients.rabbitmq.extras.multiformat._
import com.avast.clients.rabbitmq.test.ExampleEvents.{FileSource => FileSourceGpb, NewFileSourceAdded => NewFileSourceAddedGpb}
import com.avast.utils2.ByteUtils
import com.google.protobuf.ByteString
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.Future

class MultiFormatConsumerTest extends FunSuite with ScalaFutures {

  val StringFormatConverter: CheckedDeliveryConverter[String] = new CheckedDeliveryConverter[String] {
    override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.contains("text/plain")

    override def convert(d: Delivery[Bytes]): Either[ConversionException, Delivery[String]] = Right(d.copy(body = d.body.toStringUtf8))
  }

  private implicit val c: Configuration = Configuration.default.withSnakeCaseMemberNames
  private implicit val d: Decoder[Bytes] = Decoder.decodeString.map(ByteUtils.hexToBytesImmutable)

  case class FileSource(fileId: Bytes, source: String)

  case class NewFileSourceAdded(fileSources: Seq[FileSource])

  test("basic") {
    val consumer = MultiFormatConsumer.forType[Future, String](StringFormatConverter)(
      d => {
        assertResult("abc321")(d.body)
        Future.successful(DeliveryResult.Ack)
      },
      (_, _) => Future.successful(DeliveryResult.Reject)
    )

    val delivery = Delivery(
      body = Bytes.copyFromUtf8("abc321"),
      properties = MessageProperties(contentType = Some("text/plain")),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Ack)(result)
  }

  test("non-supported content-type") {
    val consumer = MultiFormatConsumer.forType[Future, String](StringFormatConverter)(
      _ => Future.successful(DeliveryResult.Ack),
      (_, _) => Future.successful(DeliveryResult.Reject)
    )

    val delivery = Delivery(
      body = Bytes.copyFromUtf8("abc321"),
      properties = MessageProperties(contentType = Some("text/javascript")),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Reject)(result)
  }

  test("json") {
    val consumer = MultiFormatConsumer.forType[Future, NewFileSourceAdded](JsonDeliveryConverter.derive())(
      d => {
        assertResult(
          NewFileSourceAdded(
            Seq(
              FileSource(Bytes.copyFromUtf8("abc"), "theSource"),
              FileSource(Bytes.copyFromUtf8("def"), "theSource")
            )))(d.body)

        Future.successful(DeliveryResult.Ack)
      },
      (_, _) => Future.successful(DeliveryResult.Reject)
    )

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

  test("gpb") {
    val consumer = MultiFormatConsumer.forType[Future, NewFileSourceAdded](
      JsonDeliveryConverter.derive(),
      GpbDeliveryConverter[NewFileSourceAddedGpb].derive()
    )(
      d => {
        assertResult(
          NewFileSourceAdded(
            Seq(
              FileSource(Bytes.copyFromUtf8("abc"), "theSource"),
              FileSource(Bytes.copyFromUtf8("def"), "theSource")
            )))(d.body)

        Future.successful(DeliveryResult.Ack)
      },
      (_, _) => Future.successful(DeliveryResult.Reject)
    )

    val delivery = Delivery(
      body = ByteStringBytes.wrap {
        NewFileSourceAddedGpb
          .newBuilder()
          .addAllFileSources(Seq(
            FileSourceGpb.newBuilder().setFileId(ByteString.copyFromUtf8("abc")).setSource("theSource").build(),
            FileSourceGpb.newBuilder().setFileId(ByteString.copyFromUtf8("def")).setSource("theSource").build()
          ).asJava)
          .build()
          .toByteString
      }: Bytes,
      properties = MessageProperties(contentType = GpbDeliveryConverter.ContentTypes.headOption.map(_.toUpperCase)),
      routingKey = ""
    )

    val result = consumer.apply(delivery).futureValue

    assertResult(DeliveryResult.Ack)(result)
  }

}
