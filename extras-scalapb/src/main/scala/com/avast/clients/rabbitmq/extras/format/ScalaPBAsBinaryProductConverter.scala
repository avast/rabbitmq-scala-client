package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.ProductConverter
import com.avast.clients.rabbitmq.api.{ConversionException, MessageProperties}
import scalapb.GeneratedMessage

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ScalaPBAsBinaryProductConverter for ${A}, try to import or define some")
trait ScalaPBAsBinaryProductConverter[A <: GeneratedMessage] extends ProductConverter[A]

object ScalaPBAsBinaryProductConverter {
  def derive[A <: GeneratedMessage: ScalaPBAsBinaryProductConverter](): ScalaPBAsBinaryProductConverter[A] =
    implicitly[ScalaPBAsBinaryProductConverter[A]]

  implicit def createBinaryProductConverter[A <: GeneratedMessage: ClassTag]: ScalaPBAsBinaryProductConverter[A] =
    new ScalaPBAsBinaryProductConverter[A] {
      override def convert(p: A): Either[ConversionException, Bytes] =
        try {
          Right(Bytes.copyFrom(p.toByteArray))
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not encode class ${implicitly[ClassTag[A]].runtimeClass.getName} to binary", e)
            }
        }

      override def fillProperties(properties: MessageProperties): MessageProperties = {
        properties.copy(
          contentType = Some("application/protobuf")
        )
      }
    }
}
