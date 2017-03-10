package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.ExecutorService
import javax.annotation.{Nonnull, Nullable}

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.{DefaultListeners, ServerChannel}
import com.avast.clients.rabbitmq.{RabbitMQConnectionConfig, RabbitMqChannelFactoryInfo, RabbitMQChannelFactory => ScalaFactory}
import com.typesafe.config.Config
import net.jodah.lyra.event.{ChannelListener, ConnectionListener, ConsumerListener}

class RabbitMQChannelFactory(scalaFactory: ScalaFactory) extends ScalaFactory {
  override def createChannel(): ServerChannel = scalaFactory.createChannel()

  override def info: RabbitMqChannelFactoryInfo = scalaFactory.info
}

object RabbitMQChannelFactory {

  /** Creates new instance of channel factory, using the passed configuration.
    *
    * @param config   The configuration.
    * @param executor [[ExecutorService]] which should be used as shared for all channels from this factory. Optional parameter.
    */
  def fromConfig(@Nonnull config: Config,
                 @Nullable executor: ExecutorService,
                 @Nullable connectionListener: ConnectionListener,
                 @Nullable channelListener: ChannelListener,
                 @Nullable consumerListener: ConsumerListener): RabbitMQChannelFactory = {
    new RabbitMQChannelFactory(
      ScalaFactory.fromConfig(
        config,
        Option(executor),
        Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
        Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
        Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener)
      ))
  }

  def create(@Nonnull connectionConfig: RabbitMQConnectionConfig,
             @Nullable executor: ExecutorService,
             @Nullable connectionListener: ConnectionListener,
             @Nullable channelListener: ChannelListener,
             @Nullable consumerListener: ConsumerListener): RabbitMQChannelFactory =
    new RabbitMQChannelFactory(
      ScalaFactory.create(
        connectionConfig,
        Option(executor),
        Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
        Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
        Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener)
      ))
}
