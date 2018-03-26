package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{Delivery, MessageProperties}

trait DeliveryConverter[A] {
  def fits(d: Delivery): Boolean

  def convert(d: Delivery): Either[ConversionException, A]
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
