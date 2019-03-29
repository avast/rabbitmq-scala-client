package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery, MessageProperties}

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
  implicit val bytesArray: DeliveryConverter[Array[Byte]] = (b: Bytes) => Right(b.toByteArray)
  implicit val utf8String: CheckedDeliveryConverter[String] = new CheckedDeliveryConverter[String] {
    override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType match {
      case Some(contentType) =>
        val ct = contentType.toLowerCase
        ct.startsWith("text") || ct.contains("charset=utf-8") || ct.contains("charset=\"utf-8\"")
      case None => true
    }
    override def convert(b: Bytes): Either[ConversionException, String] = Right(b.toStringUtf8)
  }
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
    override def fillProperties(properties: MessageProperties): MessageProperties = properties.contentType match {
      case None => properties.copy(contentType = Some("application/octet-stream"))
      case _ => properties
    }
  }
  implicit val bytesArray: ProductConverter[Array[Byte]] = new ProductConverter[Array[Byte]] {
    override def convert(p: Array[Byte]): Either[ConversionException, Bytes] = Right(Bytes.copyFrom(p))
    override def fillProperties(properties: MessageProperties): MessageProperties = properties.contentType match {
      case None => properties.copy(contentType = Some("application/octet-stream"))
      case _ => properties
    }
  }
  implicit val utf8String: ProductConverter[String] = new ProductConverter[String] {
    override def convert(p: String): Either[ConversionException, Bytes] = Right(Bytes.copyFromUtf8(p))
    override def fillProperties(properties: MessageProperties): MessageProperties = properties.contentType match {
      case None => properties.copy(contentType = Some("text/plain; charset=utf-8"))
      case _ => properties
    }
  }
}
