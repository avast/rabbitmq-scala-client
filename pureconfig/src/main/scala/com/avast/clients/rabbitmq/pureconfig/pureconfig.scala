package com.avast.clients.rabbitmq

import _root_.pureconfig._
import _root_.pureconfig.error.ConfigReaderException
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.typesafe.config.Config

import java.util.concurrent.ExecutorService
import javax.net.ssl.SSLContext
import scala.language.implicitConversions

package object pureconfig {

  private[pureconfig] val ConsumersRootName = "consumers"
  private[pureconfig] val ProducersRootName = "producers"
  private[pureconfig] val DeclarationsRootName = "declarations"

  implicit class RabbitMQConnectionOps(val f: RabbitMQConnection.type) extends AnyVal {
    def fromConfig[F[_]: ConcurrentEffect: Timer: ContextShift](config: Config,
                                                                blockingExecutor: ExecutorService,
                                                                sslContext: Option[SSLContext] = None,
                                                                connectionListener: Option[ConnectionListener[F]] = None,
                                                                channelListener: Option[ChannelListener[F]] = None,
                                                                consumerListener: Option[ConsumerListener[F]] = None)(
        implicit connectionConfigReader: ConfigReader[RabbitMQConnectionConfig] = implicits.CamelCase.connectionConfigReader,
        consumerConfigReader: ConfigReader[ConsumerConfig] = implicits.CamelCase.consumerConfigReader,
        producerConfigReader: ConfigReader[ProducerConfig] = implicits.CamelCase.producerConfigReader,
        pullConsumerConfigReader: ConfigReader[PullConsumerConfig] = implicits.CamelCase.pullConsumerConfigReader,
        streamingConsumerConfigReader: ConfigReader[StreamingConsumerConfig] = implicits.CamelCase.streamingConsumerConfigReader,
        declareExchangeConfigReader: ConfigReader[DeclareExchangeConfig] = implicits.CamelCase.declareExchangeConfigReader,
        declareQueueConfigReader: ConfigReader[DeclareQueueConfig] = implicits.CamelCase.declareQueueConfigReader,
        bindQueueConfigReader: ConfigReader[BindQueueConfig] = implicits.CamelCase.bindQueueConfigReader,
        bindExchangeConfigReader: ConfigReader[BindExchangeConfig] = implicits.CamelCase.bindExchangeConfigReader)
      : Resource[F, ConfigRabbitMQConnection[F]] = {
      val configSource = ConfigSource.fromConfig(config)

      for {
        connectionConfig <- Resource.eval(Sync[F].delay { configSource.loadOrThrow[RabbitMQConnectionConfig] })
        connection <- RabbitMQConnection.make(connectionConfig,
                                              blockingExecutor,
                                              sslContext,
                                              connectionListener,
                                              channelListener,
                                              consumerListener)
      } yield
        new DefaultConfigRabbitMQConnection[F](
          config = configSource.cursor().fold(errs => throw ConfigReaderException(errs), identity),
          wrapped = connection
        )
    }
  }
}
