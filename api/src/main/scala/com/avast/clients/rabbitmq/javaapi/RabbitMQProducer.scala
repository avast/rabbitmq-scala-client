package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes

trait RabbitMQProducer extends AutoCloseable {
  def send(routingKey: String, body: Bytes): CompletableFuture[Void]

  def send(routingKey: String, body: Bytes, properties: MessageProperties): CompletableFuture[Void]

}
