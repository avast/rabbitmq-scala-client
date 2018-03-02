package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.api.Delivery

trait FormatConverter[A] {
  def fits(d: Delivery): Boolean

  def convert(d: Delivery): Either[ConversionException, A]
}
