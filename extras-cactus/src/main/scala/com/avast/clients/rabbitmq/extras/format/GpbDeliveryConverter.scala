package com.avast.clients.rabbitmq.extras.format

import cats.syntax.either._
import com.avast.bytes.Bytes
import com.avast.cactus.CactusParser._
import com.avast.cactus.Converter
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}
import com.google.protobuf.MessageLite

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

@implicitNotFound(
  "Could not generate GpbDeliveryConverter from $GpbMessage to ${A}, try to import or define some\nMaybe you're missing some Cactus imports?")
trait GpbDeliveryConverter[GpbMessage, A] extends CheckedDeliveryConverter[A]

object GpbDeliveryConverter {
  final val ContentTypes: Set[String] = Set("application/protobuf", "application/x-protobuf")

  def apply[GpbMessage <: MessageLite]: GpbDeliveryConverterDerivator[GpbMessage] = new GpbDeliveryConverterDerivator[GpbMessage] {
    override def derive[A: GpbDeliveryConverter[GpbMessage, ?]](): GpbDeliveryConverter[GpbMessage, A] =
      implicitly[GpbDeliveryConverter[GpbMessage, A]]
  }

  trait GpbDeliveryConverterDerivator[GpbMessage <: MessageLite] {
    def derive[A: GpbDeliveryConverter[GpbMessage, ?]](): GpbDeliveryConverter[GpbMessage, A]
  }

  implicit def createGpbDeliveryConverter[GpbMessage <: MessageLite: GpbParser: Converter[?, A]: ClassTag, A: ClassTag]
    : GpbDeliveryConverter[GpbMessage, A] = new GpbDeliveryConverter[GpbMessage, A] {
    override def convert(body: Bytes): Either[ConversionException, A] = {
      implicitly[GpbParser[GpbMessage]].parseFrom(body) match {
        case Success(gpb) =>
          gpb
            .asCaseClass[A]
            .leftMap { fs =>
              ConversionException {
                s"Errors while converting to ${implicitly[ClassTag[A]].runtimeClass.getName}: ${fs.toList.mkString("[", ", ", "]")}"
              }
            }
        case Failure(NonFatal(e)) =>
          Left {
            ConversionException(s"Could not parse GPB message ${implicitly[ClassTag[GpbMessage]].runtimeClass.getName}", e)
          }
      }
    }

    override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).exists(ContentTypes.contains)
  }

}
