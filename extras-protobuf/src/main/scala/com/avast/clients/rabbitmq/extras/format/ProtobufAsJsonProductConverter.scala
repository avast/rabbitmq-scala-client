package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.ProductConverter
import com.avast.clients.rabbitmq.api.{ConversionException, MessageProperties}
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.util.JsonFormat

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ProtobufAsJsonProductConverter for ${A}, try to import or define some")
trait ProtobufAsJsonProductConverter[A <: MessageOrBuilder] extends ProductConverter[A]

object ProtobufAsJsonProductConverter {
  def derive[A <: MessageOrBuilder: ProtobufAsJsonProductConverter](): ProtobufAsJsonProductConverter[A] =
    implicitly[ProtobufAsJsonProductConverter[A]]

  // Printing is intentionally configured to use integer for enums
  // because then the serialized data is valid even after values renaming.
  private val jsonPrinter = JsonFormat.printer().printingEnumsAsInts().includingDefaultValueFields()

  implicit def createJsonProductConverter[A <: MessageOrBuilder: ClassTag]: ProtobufAsJsonProductConverter[A] =
    new ProtobufAsJsonProductConverter[A] {
      override def convert(p: A): Either[ConversionException, Bytes] =
        try {
          Right(Bytes.copyFromUtf8(jsonPrinter.print(p)))
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not encode class ${implicitly[ClassTag[A]].runtimeClass.getName} to json", e)
            }
        }

      override def fillProperties(properties: MessageProperties): MessageProperties = {
        properties.copy(
          contentType = Some("application/json")
        )
      }
    }
}
