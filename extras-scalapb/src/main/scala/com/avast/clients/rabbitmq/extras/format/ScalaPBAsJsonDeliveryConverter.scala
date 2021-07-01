package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}
import com.google.protobuf.Message
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import scalapb.json4s.Parser

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ScalaPBAsJsonDeliveryConverter for ${A}, try to import or define some")
trait ScalaPBAsJsonDeliveryConverter[A <: GeneratedMessage] extends CheckedDeliveryConverter[A]

object ScalaPBAsJsonDeliveryConverter {
  def derive[A <: GeneratedMessage: GeneratedMessageCompanion: ScalaPBAsJsonDeliveryConverter](): ScalaPBAsJsonDeliveryConverter[A] =
    implicitly[ScalaPBAsJsonDeliveryConverter[A]]

  private val jsonParser = new Parser().ignoringUnknownFields

  implicit def createJsonDeliveryConverter[A <: GeneratedMessage: GeneratedMessageCompanion: ClassTag]: ScalaPBAsJsonDeliveryConverter[A] =
    new ScalaPBAsJsonDeliveryConverter[A] {

      override def convert(body: Bytes): Either[ConversionException, A] =
        try {
          Right(jsonParser.fromJsonString[A](body.toStringUtf8))
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from json", e)
            }
        }

      override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).contains("application/json")
    }

}
