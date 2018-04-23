package com.avast.clients.rabbitmq.javaapi

import java.util.Optional
import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{RabbitMQPullConsumer => ScalaConsumer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._

import scala.concurrent.{ExecutionContext, Future}

class DefaultRabbitMQPullConsumer(scalaConsumer: ScalaConsumer[Future, Bytes] with AutoCloseable)(implicit ec: ExecutionContext)
    extends RabbitMQPullConsumer {
  override def pull(): CompletableFuture[Optional[DeliveryWithHandle]] = {
    scalaConsumer
      .pull()
      .map(_.map(_.asJava).asJava)
      .asJava
  }

  override def close(): Unit = scalaConsumer.close()
}
