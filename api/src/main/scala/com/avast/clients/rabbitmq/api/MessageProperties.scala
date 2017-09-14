package com.avast.clients.rabbitmq.api

import java.time.Instant

case class MessageProperties(contentType: Option[String] = None,
                             contentEncoding: Option[String] = None,
                             headers: Map[String, AnyRef] = Map.empty,
                             deliveryMode: Option[Integer] = None,
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
