package com.avast.clients.rabbitmq.javaapi

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties => ScalaProperties, RabbitMQProducer => ScalaProducer}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class RabbitMQProducer(scalaProducer: ScalaProducer) {
  @throws[Exception]
  def send(routingKey: String, body: Bytes): Unit = {
    scalaProducer.send(routingKey, body) match {
      case Success(_) => ()
      case Failure(NonFatal(e)) => throw e // thrown intentionally, it's Jav API!
    }
  }

  @throws[Exception]
  def send(routingKey: String, body: Bytes, properties: MessageProperties): Unit = {
    scalaProducer.send(routingKey, body, properties) match {
      case Success(_) => ()
      case Failure(NonFatal(e)) => throw e // thrown intentionally, it's Jav API!
    }
  }

  private implicit def javaPropertiesToScala(properties: MessageProperties): ScalaProperties = {
    ScalaProperties(
      Option(properties.getContentType),
      Option(properties.getContentEncoding),
      Option(properties.getHeaders).map(_.asScala.toMap).getOrElse(Map.empty),
      Option(properties.getDeliveryMode),
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
