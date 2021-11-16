package com.avast.clients.rabbitmq.extras.format

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.MultiFormatConsumer
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.ClassTag

object ScalaPBConsumer {
  def create[F[_], A <: GeneratedMessage: GeneratedMessageCompanion: ClassTag](
      action: Delivery[A] => F[DeliveryResult]): (Delivery[Bytes] => F[DeliveryResult]) =
    MultiFormatConsumer.forType[F, A](ScalaPBAsJsonDeliveryConverter.derive(), ScalaPBAsBinaryDeliveryConverter.derive())(action)
}
