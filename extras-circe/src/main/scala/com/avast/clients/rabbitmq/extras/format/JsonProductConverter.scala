package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.MessageProperties
import com.avast.clients.rabbitmq.{ConversionException, ProductConverter}
import io.circe.Encoder
import io.circe.syntax._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound(
  "Could not generate JsonProductConverter for ${A}, try to import or define some\nMaybe you're missing some circe imports?")
trait JsonProductConverter[A] extends ProductConverter[A]

object JsonProductConverter {
  def derive[A: JsonProductConverter](): JsonProductConverter[A] = implicitly[JsonProductConverter[A]]

  implicit def createJsonProductConverter[A: Encoder: ClassTag]: JsonProductConverter[A] = new JsonProductConverter[A] {
    override def convert(p: A): Either[ConversionException, Bytes] = {
      try {
        Right(Bytes.copyFromUtf8(p.asJson.noSpaces))
      } catch {
        case NonFatal(e) =>
          Left {
            ConversionException(s"Could not encode class ${implicitly[ClassTag[A]].runtimeClass.getName} to json", e)
          }
      }
    }

    override def fillProperties(properties: MessageProperties): MessageProperties = {
      properties.copy(
        contentType = Some("application/json")
      )
    }
  }
}
