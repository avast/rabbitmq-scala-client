package com.avast.clients.rabbitmq

import java.util.concurrent.ExecutorService

import _root_.pureconfig._
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.typesafe.config.Config

import scala.language.{higherKinds, implicitConversions}

package object pureconfig {

  object RabbitMQConnectionOps {
    def fromConfig[F[_]: ConcurrentEffect: Timer: ContextShift](
        config: Config,
        blockingExecutor: ExecutorService,
        connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
        channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
        consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener)(
        implicit namingConvention: NamingConvention = CamelCase): Resource[F, ConfigRabbitMQConnection[F]] = {

      implicit val pureconfigImplicits: PureconfigImplicits = new PureconfigImplicits
      import pureconfigImplicits._

      for {
        connectionConfig <- Resource.liftF(Sync[F].delay { ConfigSource.fromConfig(config).loadOrThrow[RabbitMQConnectionConfig] })
        connection <- RabbitMQConnection.make(connectionConfig, blockingExecutor, connectionListener, channelListener, consumerListener)
      } yield new DefaultConfigRabbitMQConnection[F](config, connection)
    }
  }

  // to add the extension method to RabbitMQConnection object:
  implicit def connectionObjectToOps(f: RabbitMQConnection.type): RabbitMQConnectionOps.type = RabbitMQConnectionOps
}
