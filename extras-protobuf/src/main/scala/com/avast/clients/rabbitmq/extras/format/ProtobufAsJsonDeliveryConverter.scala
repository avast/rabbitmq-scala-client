package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{GeneratedMessageV3, Message}

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ProtobufDeliveryConverter for ${A}, try to import or define some")
trait ProtobufAsJsonDeliveryConverter[A <: GeneratedMessageV3] extends CheckedDeliveryConverter[A]

object ProtobufAsJsonDeliveryConverter {
  def derive[A <: GeneratedMessageV3: ProtobufAsJsonDeliveryConverter](): ProtobufAsJsonDeliveryConverter[A] =
    implicitly[ProtobufAsJsonDeliveryConverter[A]]

  private val jsonParser = JsonFormat.parser().ignoringUnknownFields()

  implicit def createJsonDeliveryConverter[A <: GeneratedMessageV3: ClassTag]: ProtobufAsJsonDeliveryConverter[A] =
    new ProtobufAsJsonDeliveryConverter[A] {

      private val newBuilderMethod = implicitly[ClassTag[A]].runtimeClass.getMethod("newBuilder")

      override def convert(body: Bytes): Either[ConversionException, A] =
        try {
          val builder = newBuilderMethod.invoke(null).asInstanceOf[Message.Builder]
          jsonParser.merge(body.toStringUtf8, builder)
          Right(builder.build().asInstanceOf[A])
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from json", e)
            }
        }

      override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).contains("application/json")
    }

}
