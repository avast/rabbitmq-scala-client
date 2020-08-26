package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}
import com.google.protobuf.{GeneratedMessageV3, Message}

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.util.control.NonFatal

@implicitNotFound("Could not generate ProtobufAsBinaryDeliveryConverter for ${A}, try to import or define some")
trait ProtobufAsBinaryDeliveryConverter[A <: GeneratedMessageV3] extends CheckedDeliveryConverter[A]

object ProtobufAsBinaryDeliveryConverter {
  def derive[A <: GeneratedMessageV3: ProtobufAsBinaryDeliveryConverter](): ProtobufAsBinaryDeliveryConverter[A] =
    implicitly[ProtobufAsBinaryDeliveryConverter[A]]

  implicit def createBinaryDeliveryConverter[A <: GeneratedMessageV3: ClassTag]: ProtobufAsBinaryDeliveryConverter[A] =
    new ProtobufAsBinaryDeliveryConverter[A] {

      private val newBuilderMethod = implicitly[ClassTag[A]].runtimeClass.getMethod("newBuilder")

      override def convert(body: Bytes): Either[ConversionException, A] =
        try {
          val builder = newBuilderMethod.invoke(null).asInstanceOf[Message.Builder]
          Right(builder.mergeFrom(body.toByteArray).build().asInstanceOf[A])
        } catch {
          case NonFatal(e) =>
            Left {
              ConversionException(s"Could not decode class ${implicitly[ClassTag[A]].runtimeClass.getName} from binary", e)
            }
        }

      override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.map(_.toLowerCase).exists(ct => ct == "application/protobuf" || ct == "application/x-protobuf")
    }

}
