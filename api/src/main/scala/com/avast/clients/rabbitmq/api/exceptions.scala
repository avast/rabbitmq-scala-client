package com.avast.clients.rabbitmq.api

case class ConversionException(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)
