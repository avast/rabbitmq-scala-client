package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{Delivery, MessageProperties}

import scala.annotation.implicitNotFound

/** Tries to convert `Delivery[Bytes]` to `Delivery[A]`.
  */
@implicitNotFound("Could not find DeliveryConverter for ${A}, try to import or define some")
trait DeliveryConverter[A] {
  def convert(d: Bytes): Either[ConversionException, A]
}

/** Tries to convert `Delivery[Bytes]` to `Delivery[A]`. Contains check whether the `Delivery[Bytes]` fits for the conversion.
  */
@implicitNotFound("Could not find CheckedDeliveryConverter for ${A}, try to import or define some")
trait CheckedDeliveryConverter[A] extends DeliveryConverter[A] {
  def canConvert(d: Delivery[Bytes]): Boolean
}

object DeliveryConverter {
  implicit val identity: DeliveryConverter[Bytes] = (b: Bytes) => Right(b)
}

@implicitNotFound("Could not find ProductConverter for ${A}, try to import or define some")
trait ProductConverter[A] {
  def convert(p: A): Either[ConversionException, Bytes]

  /** Fills some properties for the producer's message. It's usually just Content-Type but that is implementation-specific.
    */
  def fillProperties(properties: MessageProperties): MessageProperties
}

object ProductConverter {
  implicit val identity: ProductConverter[Bytes] = new ProductConverter[Bytes] {
    override def convert(p: Bytes): Either[ConversionException, Bytes] = Right(p)

    override def fillProperties(properties: MessageProperties): MessageProperties = properties
  }
}

case class ConversionException(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)
