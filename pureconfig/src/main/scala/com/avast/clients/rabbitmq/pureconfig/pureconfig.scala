package com.avast.clients.rabbitmq

import java.util.concurrent.ExecutorService

import _root_.pureconfig._
import _root_.pureconfig.generic.ProductHint
import _root_.pureconfig.generic.semiauto._
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.typesafe.config.Config

import scala.language.{higherKinds, implicitConversions}

package object pureconfig extends Implicits {

  object RabbitMQConnectionOps {
    def fromConfig[F[_]: ConcurrentEffect: Timer: ContextShift](
        config: Config,
        blockingExecutor: ExecutorService,
        connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
        channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
        consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener): Resource[F, PureconfigRabbitMQConnection[F]] = {

      Resource
        .liftF(Sync[F].delay {
          ConfigSource.fromConfig(config).loadOrThrow[RabbitMQConnectionConfig]
        })
        .flatMap { config =>
          RabbitMQConnection.make(config, blockingExecutor, connectionListener, channelListener, consumerListener)
        }
        .map(new DefaultPureconfigRabbitMQConnection[F](config, _))
    }
  }

  implicit def connectionObjectToOps(f: RabbitMQConnection.type): RabbitMQConnectionOps.type = RabbitMQConnectionOps
}
