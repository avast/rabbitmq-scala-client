package com.avast.clients.rabbitmq.api
import java.io.IOException

case class ConversionException(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)

case class ChannelNotRecoveredException(desc: String, cause: Throwable = null) extends IOException(desc, cause)

case class TooBigMessage(desc: String, cause: Throwable = null) extends IllegalArgumentException(desc, cause)

case class MaxAttempts(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)

case class NotAcknowledgedPublish(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)
