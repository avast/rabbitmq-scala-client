package com.avast.clients.rabbitmq.publisher

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.implicits.toFunctorOps
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.MessageProperties
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.{CorrelationId, ProductConverter, ServerChannel}
import com.avast.metrics.scalaeffectapi.Monitor

class DefaultRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                         exchangeName: String,
                                                         channel: ServerChannel,
                                                         defaultProperties: MessageProperties,
                                                         reportUnroutable: Boolean,
                                                         sizeLimitBytes: Option[Int],
                                                         blocker: Blocker,
                                                         logger: ImplicitContextLogger[F],
                                                         monitor: Monitor[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F])
    extends BaseRabbitMQProducer[F, A](name,
                                       exchangeName,
                                       channel,
                                       defaultProperties,
                                       reportUnroutable,
                                       sizeLimitBytes,
                                       blocker,
                                       logger,
                                       monitor) {
  override def sendMessage(routingKey: String, body: Bytes, properties: MessageProperties)(implicit correlationId: CorrelationId): F[Unit] =
    basicSend(routingKey, body, properties).void
}
