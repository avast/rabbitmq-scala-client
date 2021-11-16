package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.CorrelationIdHeaderName
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

private[rabbitmq] case class DeliveryMetadata(messageId: MessageId,
                                              correlationId: CorrelationId,
                                              deliveryTag: DeliveryTag,
                                              routingKey: RoutingKey,
                                              fixedProperties: BasicProperties)

private[rabbitmq] object DeliveryMetadata {
  def from(envelope: Envelope, properties: BasicProperties): DeliveryMetadata = {
    val correlationIdRaw = Option(properties.getCorrelationId).orElse {
      Option(properties.getHeaders).flatMap(h => Option(h.get(CorrelationIdHeaderName))).map(_.toString)
    }

    val fixedProperties = properties.builder().correlationId(correlationIdRaw.orNull).build()

    val correlationId = CorrelationId(correlationIdRaw.getOrElse("-none-"))
    val messageId = MessageId(Option(fixedProperties.getMessageId).getOrElse("-none-"))

    val deliveryTag = DeliveryTag(envelope.getDeliveryTag)
    val routingKey = RoutingKey(fixedProperties.getOriginalRoutingKey.getOrElse(envelope.getRoutingKey))

    DeliveryMetadata(messageId, correlationId, deliveryTag, routingKey, fixedProperties)
  }
}

private[rabbitmq] final case class MessageId(value: String) extends AnyVal
private[rabbitmq] final case class CorrelationId(value: String) extends AnyVal
private[rabbitmq] final case class RoutingKey(value: String) extends AnyVal
private[rabbitmq] final case class DeliveryTag(value: Long) extends AnyVal
