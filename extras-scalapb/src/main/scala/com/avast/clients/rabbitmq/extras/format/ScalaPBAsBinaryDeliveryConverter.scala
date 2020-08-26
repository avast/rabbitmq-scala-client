package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ScalaPBAsBinaryDeliveryConverter for ${A}, try to import or define some")
trait ScalaPBAsBinaryDeliveryConverter[A <: GeneratedMessage] extends CheckedDeliveryConverter[A]

object ScalaPBAsBinaryDeliveryConverter {
  def derive[A <: GeneratedMessage: GeneratedMessageCompanion: ScalaPBAsBinaryDeliveryConverter](): ScalaPBAsBinaryDeliveryConverter[A] =
    implicitly[ScalaPBAsBinaryDeliveryConverter[A]]

  implicit def createBinaryDeliveryConverter[A <: GeneratedMessage: GeneratedMessageCompanion: ClassTag]: ScalaPBAsBinaryDeliveryConverter[A] =
    new ScalaPBAsBinaryDeliveryConverter[A] {

      override def convert(body: Bytes): Either[ConversionException, A] =
        try {
          Right(implicitly[GeneratedMessageCompanion[A]].parseFrom(body.toByteArray))
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from binary", e)
            }
        }

      override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).exists(ct => ct == "application/protobuf" || ct == "application/x-protobuf")
    }

}
