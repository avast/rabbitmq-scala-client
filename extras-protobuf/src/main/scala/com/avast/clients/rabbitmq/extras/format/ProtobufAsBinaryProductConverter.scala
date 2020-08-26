package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.bytes.gpb.ByteStringBytes
import com.avast.clients.rabbitmq.ProductConverter
import com.avast.clients.rabbitmq.api.{ConversionException, MessageProperties}
import com.google.protobuf.MessageLite

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ProtobufAsBinaryProductConverter for ${A}, try to import or define some")
trait ProtobufAsBinaryProductConverter[A <: MessageLite] extends ProductConverter[A]

object ProtobufAsBinaryProductConverter {
  def derive[A <: MessageLite: ProtobufAsBinaryProductConverter](): ProtobufAsBinaryProductConverter[A] =
    implicitly[ProtobufAsBinaryProductConverter[A]]

  implicit def createBinaryProductConverter[A <: MessageLite: ClassTag]: ProtobufAsBinaryProductConverter[A] =
    new ProtobufAsBinaryProductConverter[A] {
      override def convert(p: A): Either[ConversionException, Bytes] =
        try {
          Right(ByteStringBytes.wrap(p.toByteString))
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
