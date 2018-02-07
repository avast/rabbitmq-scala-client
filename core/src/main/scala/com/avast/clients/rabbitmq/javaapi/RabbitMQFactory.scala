package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{ExecutorService, ScheduledExecutorService}

import com.avast.clients.rabbitmq.RabbitMQFactory.DefaultListeners
import com.avast.clients.rabbitmq.{
  ChannelListener,
  ConnectionListener,
  ConsumerListener,
  RabbitMQConnectionConfig,
  RabbitMQFactory => ScalaFactory
}
import com.avast.utils2.errorhandling.FutureTimeouter
import com.typesafe.config.Config

object RabbitMQFactory {

  def newBuilder(config: Config): Builder[RabbitMQJavaFactory] = {
    new Builder(Left(config))
  }

  def newBuilder(connectionConfig: RabbitMQConnectionConfig): Builder[RabbitMQJavaFactoryManual] = {
    new Builder(Right(connectionConfig))
  }

  //scalastyle:off
  class Builder[B <: RabbitMQJavaFactoryManual] private[RabbitMQFactory] (config: Either[Config, RabbitMQConnectionConfig]) {
    private var executor: Option[ExecutorService] = None
    private var connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener
    private var channelListener: ChannelListener = DefaultListeners.DefaultChannelListener
    private var consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener
    private var scheduledExecutorService: ScheduledExecutorService = FutureTimeouter.Implicits.DefaultScheduledExecutor

    def withExecutor(executor: ExecutorService): Builder[B] = {
      this.executor = Option(executor)
      this
    }

    def withConnectionListener(connectionListener: ConnectionListener): Builder[B] = {
      this.connectionListener = connectionListener
      this
    }

    def withChannelListener(channelListener: ChannelListener): Builder[B] = {
      this.channelListener = channelListener
      this
    }

    def withConsumerListener(consumerListener: ConsumerListener): Builder[B] = {
      this.consumerListener = consumerListener
      this
    }

    def withScheduledExecutorService(ses: ScheduledExecutorService): Builder[B] = {
      this.scheduledExecutorService = ses
      this
    }

    def build(): B = {
      config match {
        case Left(providedConfig) =>
          new RabbitMQJavaFactoryImpl(
            ScalaFactory
              .fromConfig(
                providedConfig,
                executor,
                Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
                Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
                Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener),
                scheduledExecutorService = scheduledExecutorService
              )).asInstanceOf[B]

        case Right(connectionConfig) =>
          new RabbitMQJavaFactoryImpl(
            ScalaFactory
              .create(
                connectionConfig,
                executor,
                Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
                Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
                Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener),
                scheduledExecutorService = scheduledExecutorService
              )
              .asInstanceOf[ScalaFactory]) // this is a little bit hacky... :-/
            .asInstanceOf[B]
      }
    }

  }

}
