package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.MultiFormatConsumer
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.google.protobuf.GeneratedMessageV3

import scala.language.higherKinds
import scala.reflect.ClassTag

object ProtobufConsumer {
  def create[F[_], A <: GeneratedMessageV3: ClassTag](action: Delivery[A] => F[DeliveryResult]): (Delivery[Bytes] => F[DeliveryResult]) =
    MultiFormatConsumer.forType[F, A](
      ProtobufAsJsonDeliveryConverter.derive(),
      ProtobufAsBinaryDeliveryConverter.derive())(action)
}
