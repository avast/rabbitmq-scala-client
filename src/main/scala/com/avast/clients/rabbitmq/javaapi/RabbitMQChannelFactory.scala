package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.ExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.{DefaultListeners, ServerChannel}
import com.avast.clients.rabbitmq.api.{ChannelListener, ConnectionListener, ConsumerListener}
import com.avast.clients.rabbitmq.{RabbitMQConnectionConfig, RabbitMqChannelFactoryInfo, RabbitMQChannelFactory => ScalaFactory}
import com.typesafe.config.Config

class RabbitMQChannelFactory(scalaFactory: ScalaFactory) extends ScalaFactory {
  override def createChannel(): ServerChannel = scalaFactory.createChannel()

  override def info: RabbitMqChannelFactoryInfo = scalaFactory.info

  override def connectionListener: ConnectionListener = scalaFactory.connectionListener

  override def channelListener: ChannelListener = scalaFactory.channelListener

  override def consumerListener: ConsumerListener = scalaFactory.consumerListener
}

object RabbitMQChannelFactory {

  def newBuilder(config: Config): Builder = {
    new Builder(Left(config))
  }

  def newBuilder(connectionConfig: RabbitMQConnectionConfig): Builder = {
    new Builder(Right(connectionConfig))
  }

  //scalastyle:off
  class Builder private[RabbitMQChannelFactory] (config: Either[Config, RabbitMQConnectionConfig]) {
    private var executor: Option[ExecutorService] = None
    private var connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener
    private var channelListener: ChannelListener = DefaultListeners.DefaultChannelListener
    private var consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener

    def withExecutor(executor: ExecutorService): Builder = {
      this.executor = Option(executor)
      this
    }

    def withConnectionListener(connectionListener: ConnectionListener): Builder = {
      this.connectionListener = connectionListener
      this
    }

    def withChannelListener(channelListener: ChannelListener): Builder = {
      this.channelListener = channelListener
      this
    }

    def withConsumerListener(consumerListener: ConsumerListener): Builder = {
      this.consumerListener = consumerListener
      this
    }

    def build(): RabbitMQChannelFactory = {
      config match {
        case Left(providedConfig) =>
          new RabbitMQChannelFactory(
            ScalaFactory.fromConfig(
              providedConfig,
              executor,
              Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
              Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
              Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener)
            ))

        case Right(connectionConfig) =>
          new RabbitMQChannelFactory(
            ScalaFactory.create(
              connectionConfig,
              executor,
              Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
              Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
              Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener)
            ))
      }
    }

  }

}
