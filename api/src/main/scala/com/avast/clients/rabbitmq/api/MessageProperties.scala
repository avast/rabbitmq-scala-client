package com.avast.clients.rabbitmq.api

import java.time.Instant

case class MessageProperties(contentType: Option[String] = None,
                             contentEncoding: Option[String] = None,
                             headers: Map[String, AnyRef] = Map.empty,
                             deliveryMode: Option[DeliveryMode] = None,
                             priority: Option[Integer] = None,
                             correlationId: Option[String] = None,
                             replyTo: Option[String] = None,
                             expiration: Option[String] = None,
                             messageId: Option[String] = None,
                             timestamp: Option[Instant] = None,
                             `type`: Option[String] = None,
                             userId: Option[String] = None,
                             appId: Option[String] = None,
                             clusterId: Option[String] = None)

object MessageProperties {
  val empty: MessageProperties = MessageProperties()
}

sealed trait DeliveryMode {
  def code: Integer
}
object DeliveryMode {
  case object NonPersistent extends DeliveryMode {
    override def code: Integer = 1
  }
  case object Persistent extends DeliveryMode {
    override def code: Integer = 2
  }
  def fromCode(code: Integer): Option[DeliveryMode] = code.toInt match {
    case 0 => None
    case 1 => Some(NonPersistent)
    case 2 => Some(Persistent)
    case _ => throw new IllegalArgumentException(s"Unknown delivery mode: $code")
  }
}
