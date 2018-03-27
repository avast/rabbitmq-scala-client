package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{Delivery, MessageProperties}

import scala.annotation.implicitNotFound

@implicitNotFound("Could not find DeliveryConverter for ${A}, try to import or define some")
trait DeliveryConverter[A] {
  def convert(d: Delivery[Bytes]): Either[ConversionException, Delivery[A]]
}

trait DeliveryConverterCheck {
  def canConvert(d: Delivery[Bytes]): Boolean
}

@implicitNotFound("Could not find CheckedDeliveryConverter for ${A}, try to import or define some")
trait CheckedDeliveryConverter[A] extends DeliveryConverter[A] with DeliveryConverterCheck

object DeliveryConverter {
  implicit val identity: DeliveryConverter[Bytes] = (d: Delivery[Bytes]) => Right(d)
}

trait ProductConverter[A] {
  def convert(p: A): Either[ConversionException, Bytes]

  def fillProperties(properties: MessageProperties): MessageProperties
}

object ProductConverter {
  implicit val identity: ProductConverter[Bytes] = new ProductConverter[Bytes] {
    override def convert(p: Bytes): Either[ConversionException, Bytes] = Right(p)

    override def fillProperties(properties: MessageProperties): MessageProperties = properties
  }
}

case class ConversionException(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)
