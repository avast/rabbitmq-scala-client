package com.avast.clients.rabbitmq.extras.format

import cats.syntax.either._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.Delivery
import com.avast.clients.rabbitmq.{CheckedDeliveryConverter, ConversionException}
import io.circe.Decoder
import io.circe.parser.decode

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound(
  "Could not generate JsonDeliveryConverter for ${A}, try to import or define some\nMaybe you're missing some circe imports?")
trait JsonDeliveryConverter[A] extends CheckedDeliveryConverter[A]

object JsonDeliveryConverter {
  def derive[A: JsonDeliveryConverter](): JsonDeliveryConverter[A] = implicitly[JsonDeliveryConverter[A]]

  implicit def createJsonDeliveryConverter[A: Decoder: ClassTag]: JsonDeliveryConverter[A] = new JsonDeliveryConverter[A] {
    override def convert(body: Bytes): Either[ConversionException, A] = {
      decode[A](body.toStringUtf8)
        .leftMap {
          ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from json", _)
        }
    }

    override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).contains("application/json")
  }

}
