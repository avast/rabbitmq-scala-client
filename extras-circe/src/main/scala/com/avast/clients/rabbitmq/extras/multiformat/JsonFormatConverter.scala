package com.avast.clients.rabbitmq.extras.multiformat

import cats.syntax.either._
import com.avast.clients.rabbitmq.api.Delivery
import com.avast.clients.rabbitmq.{ConversionException, FormatConverter}
import io.circe.Decoder
import io.circe.parser.decode

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound("Could not generate JsonFormatConverter for $A, try to import or define some")
trait JsonFormatConverter[A] extends FormatConverter[A]

object JsonFormatConverter {
  def derive[A: JsonFormatConverter](): JsonFormatConverter[A] = implicitly[JsonFormatConverter[A]]

  implicit def createJsonFormatConverter[A: Decoder: ClassTag]: JsonFormatConverter[A] = new JsonFormatConverter[A] {
    override def convert(d: Delivery): Either[ConversionException, A] = {
      decode[A](d.body.toStringUtf8).leftMap {
        ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from json", _)
      }
    }

    override def fits(d: Delivery): Boolean = d.properties.contentType.map(_.toLowerCase).contains("application/json")
  }

}
