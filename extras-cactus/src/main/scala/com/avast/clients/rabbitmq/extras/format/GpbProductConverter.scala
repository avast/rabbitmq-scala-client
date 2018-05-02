package com.avast.clients.rabbitmq.extras.format

import cats.syntax.either._
import com.avast.bytes.Bytes
import com.avast.bytes.gpb.ByteStringBytes
import com.avast.cactus.CactusParser._
import com.avast.cactus.Converter
import com.avast.clients.rabbitmq.api.MessageProperties
import com.avast.clients.rabbitmq.{ConversionException, ProductConverter}
import com.google.protobuf.MessageLite

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound(
  "Could not generate GpbProductConverter from $GpbMessage to ${A}, try to import or define some\nMaybe you're missing some Cactus imports?")
trait GpbProductConverter[GpbMessage, A] extends ProductConverter[A]

object GpbProductConverter {
  def apply[GpbMessage <: MessageLite]: GpbProductConverterDerivator[GpbMessage] = new GpbProductConverterDerivator[GpbMessage] {
    override def derive[A: GpbProductConverter[GpbMessage, ?]](): GpbProductConverter[GpbMessage, A] =
      implicitly[GpbProductConverter[GpbMessage, A]]
  }

  trait GpbProductConverterDerivator[GpbMessage <: MessageLite] {
    def derive[A: GpbProductConverter[GpbMessage, ?]](): GpbProductConverter[GpbMessage, A]
  }

  implicit def createGpbDeliveryConverter[GpbMessage <: MessageLite: Converter[A, ?]: ClassTag, A: ClassTag]
    : GpbProductConverter[GpbMessage, A] = new GpbProductConverter[GpbMessage, A] {
    override def convert(p: A): Either[ConversionException, Bytes] = {
      try {
        p.asGpb[GpbMessage]
          .map(gpb => ByteStringBytes.wrap(gpb.toByteString))
          .leftMap { fs =>
            ConversionException {
              s"Errors while converting ${implicitly[ClassTag[A]].runtimeClass.getName} to " +
                s"GPB ${implicitly[ClassTag[GpbMessage]].runtimeClass.getName}: ${fs.toList
                  .mkString("[", ", ", "]")}"
            }
          }
      } catch {
        case NonFatal(e) =>
          Left {
            ConversionException(
              s"Could not convert ${implicitly[ClassTag[A]].runtimeClass.getName} to GPB ${implicitly[ClassTag[GpbMessage]].runtimeClass.getName}",
              e
            )
          }
      }
    }

    override def fillProperties(properties: MessageProperties): MessageProperties = {
      properties.copy(
        contentType = Some("application/protobuf")
      )
    }
  }
}
