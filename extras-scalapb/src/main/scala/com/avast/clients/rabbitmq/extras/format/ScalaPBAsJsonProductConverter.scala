package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.ProductConverter
import com.avast.clients.rabbitmq.api.{ConversionException, MessageProperties}
import scalapb.GeneratedMessage
import scalapb.json4s.Printer

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ScalaPBAsJsonProductConverter for ${A}, try to import or define some")
trait ScalaPBAsJsonProductConverter[A <: GeneratedMessage] extends ProductConverter[A]

object ScalaPBAsJsonProductConverter {
  def derive[A <: GeneratedMessage: ScalaPBAsJsonProductConverter](): ScalaPBAsJsonProductConverter[A] =
    implicitly[ScalaPBAsJsonProductConverter[A]]

  // Printing is intentionally configured to use integer for enums
  // because then the serialized data is valid even after values renaming.
  private val jsonPrinter = new Printer().formattingEnumsAsNumber.includingDefaultValueFields

  implicit def createJsonProductConverter[A <: GeneratedMessage: ClassTag]: ScalaPBAsJsonProductConverter[A] =
    new ScalaPBAsJsonProductConverter[A] {
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
