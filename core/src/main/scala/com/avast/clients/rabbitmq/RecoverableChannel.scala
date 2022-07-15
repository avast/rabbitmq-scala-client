package com.avast.clients.rabbitmq

import cats.effect.{Resource, Sync}
import cats.effect.concurrent.Ref
import cats.implicits.{catsSyntaxFlatMapOps, toFlatMapOps}

private[rabbitmq] class RecoverableChannel[F[_]: Sync] private (connection: RabbitMQConnection[F],
                                                                channelRef: Ref[F, ServerChannel],
                                                                channelClose: Ref[F, F[Unit]]) {
  val get: F[ServerChannel] = channelRef.get

  val recover: F[Unit] = {
    connection.newChannel().allocated.flatMap {
      case (channel, chclose) => channelRef.set(channel) >> channelClose.getAndSet(chclose).flatten
    }
  }
}

object RecoverableChannel {
  def make[F[_]: Sync](connection: RabbitMQConnection[F]): Resource[F, RecoverableChannel[F]] = {
    ???
  }
}
