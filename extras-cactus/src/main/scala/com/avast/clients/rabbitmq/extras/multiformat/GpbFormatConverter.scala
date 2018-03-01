package com.avast.clients.rabbitmq.extras.multiformat

import cats.syntax.either._
import com.avast.cactus.CactusParser._
import com.avast.cactus.Converter
import com.avast.clients.rabbitmq.api.Delivery
import com.avast.clients.rabbitmq.{ConversionException, FormatConverter}
import com.google.protobuf.MessageLite

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait GpbFormatConverter[GpbMessage, A] extends FormatConverter[A]

object GpbFormatConverter {
  final val ContentTypes: Set[String] = Set("application/protobuf", "application/x-protobuf")

  def apply[GpbMessage <: MessageLite]: GpbConverterDerivator[GpbMessage] = new GpbConverterDerivator[GpbMessage] {
    override def derive[A: GpbFormatConverter[GpbMessage, ?]](): GpbFormatConverter[GpbMessage, A] =
      implicitly[GpbFormatConverter[GpbMessage, A]]
  }

  trait GpbConverterDerivator[GpbMessage <: MessageLite] {
    def derive[A: GpbFormatConverter[GpbMessage, ?]](): GpbFormatConverter[GpbMessage, A]
  }

  implicit def createGpbFormatConverter[GpbMessage <: MessageLite: GpbParser: Converter[?, A]: ClassTag, A: ClassTag]
    : GpbFormatConverter[GpbMessage, A] = new GpbFormatConverter[GpbMessage, A] {
    override def convert(d: Delivery): Either[ConversionException, A] = {
      implicitly[GpbParser[GpbMessage]].parseFrom(d.body) match {
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

    override def fits(d: Delivery): Boolean = d.properties.contentType.map(_.toLowerCase).exists(ContentTypes.contains)
  }

}
