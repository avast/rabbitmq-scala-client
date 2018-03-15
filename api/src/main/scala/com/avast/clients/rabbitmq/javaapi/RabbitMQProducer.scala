package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes

import scala.collection.JavaConverters._
import scala.language.implicitConversions

trait RabbitMQProducer extends AutoCloseable {
  @throws[Exception]
  def send(routingKey: String, body: Bytes): CompletableFuture[Void]

  @throws[Exception]
  def send(routingKey: String, body: Bytes, properties: MessageProperties): CompletableFuture[Void]

}
