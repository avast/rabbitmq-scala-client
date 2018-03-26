package com.avast.clients.rabbitmq.extras.multiformat

import cats.syntax.either._
import com.avast.clients.rabbitmq.api.Delivery
import com.avast.clients.rabbitmq.{ConversionException, DeliveryConverter}
import io.circe.Decoder
import io.circe.parser.decode

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound("Could not generate JsonFormatConverter for $A, try to import or define some")
trait JsonDeliveryConverter[A] extends DeliveryConverter[A]

object JsonDeliveryConverter {
  def derive[A: JsonDeliveryConverter](): JsonDeliveryConverter[A] = implicitly[JsonDeliveryConverter[A]]

  implicit def createJsonFormatConverter[A: Decoder: ClassTag]: JsonDeliveryConverter[A] = new JsonDeliveryConverter[A] {
    override def convert(d: Delivery): Either[ConversionException, A] = {
      decode[A](d.body.toStringUtf8).leftMap {
        ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from json", _)
      }
    }

    override def fits(d: Delivery): Boolean = d.properties.contentType.map(_.toLowerCase).contains("application/json")
  }

}
