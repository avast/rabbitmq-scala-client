package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes

sealed trait Delivery[+A] {
  def properties: MessageProperties

  def routingKey: String
}

object Delivery {

  case class Ok[+A](body: A, properties: MessageProperties, routingKey: String) extends Delivery[A]

  case class MalformedContent(body: Bytes, properties: MessageProperties, routingKey: String, ce: ConversionException)
      extends Delivery[Nothing]

  def apply[A](body: A, properties: MessageProperties, routingKey: String): Delivery.Ok[A] = {
    Delivery.Ok(body, properties, routingKey)
  }
}
