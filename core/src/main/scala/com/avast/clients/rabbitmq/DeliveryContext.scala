package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.CorrelationIdHeaderName
import com.avast.clients.rabbitmq.api.CorrelationIdStrategy.CorrelationIdKeyName
import com.avast.clients.rabbitmq.api.Delivery
import com.avast.clients.rabbitmq.logging.LoggingContext
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

private[rabbitmq] case class DeliveryWithContext[A](delivery: Delivery[A], implicit val context: DeliveryContext)

private[rabbitmq] case class DeliveryContext(messageId: MessageId,
                                             correlationId: Option[CorrelationId],
                                             deliveryTag: DeliveryTag,
                                             routingKey: RoutingKey,
                                             fixedProperties: BasicProperties)
    extends LoggingContext {

  override lazy val asContextMap: Map[String, String] = {
    correlationId.map(_.asContextMap).getOrElse(Map.empty)
  }
}

private[rabbitmq] object DeliveryContext {
  def from(envelope: Envelope, properties: BasicProperties): DeliveryContext = {
    val correlationIdRaw = Option(properties.getCorrelationId).orElse {
      Option(properties.getHeaders).flatMap(h => Option(h.get(CorrelationIdHeaderName))).map(_.toString)
    }

    val fixedProperties = properties.builder().correlationId(correlationIdRaw.orNull).build()

    val correlationId = correlationIdRaw.map(CorrelationId)
    val messageId = MessageId(Option(fixedProperties.getMessageId).getOrElse("-none-"))

    val deliveryTag = DeliveryTag(envelope.getDeliveryTag)
    val routingKey = RoutingKey(fixedProperties.getOriginalRoutingKey.getOrElse(envelope.getRoutingKey))

    DeliveryContext(messageId, correlationId, deliveryTag, routingKey, fixedProperties)
  }
}

private[rabbitmq] final case class CorrelationId(value: String) extends LoggingContext {
  def asContextMap: Map[String, String] = Map(CorrelationIdKeyName -> value)
}

private[rabbitmq] final case class MessageId(value: String) extends AnyVal
private[rabbitmq] final case class RoutingKey(value: String) extends AnyVal
private[rabbitmq] final case class DeliveryTag(value: Long) extends AnyVal
