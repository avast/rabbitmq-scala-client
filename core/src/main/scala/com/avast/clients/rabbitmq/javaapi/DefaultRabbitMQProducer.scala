package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{DeliveryMode, MessageProperties => ScalaProperties, RabbitMQProducer => ScalaProducer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

abstract class DefaultRabbitMQProducer private[rabbitmq] (scalaProducer: ScalaProducer[Future, Bytes])(implicit ec: ExecutionContext)
    extends RabbitMQProducer {
  def send(routingKey: String, body: Bytes): CompletableFuture[Void] = {
    scalaProducer.send(routingKey, body, None).map(_ => null: Void).asJava
  }

  def send(routingKey: String, body: Bytes, properties: MessageProperties): CompletableFuture[Void] = {
    scalaProducer.send(routingKey, body, Some(properties)).map(_ => null: Void).asJava
  }

  private implicit def javaPropertiesToScala(properties: MessageProperties): ScalaProperties = {
    ScalaProperties(
      Option(properties.getContentType),
      Option(properties.getContentEncoding),
      Option(properties.getHeaders).map(_.asScala.toMap).getOrElse(Map.empty),
      DeliveryMode.fromCode(properties.getDeliveryMode),
      Option(properties.getPriority),
      Option(properties.getCorrelationId),
      Option(properties.getReplyTo),
      Option(properties.getExpiration),
      Option(properties.getMessageId),
      Option(properties.getTimestamp).map(_.toInstant),
      Option(properties.getType),
      Option(properties.getUserId),
      Option(properties.getAppId),
      Option(properties.getClusterId)
    )
  }

}
