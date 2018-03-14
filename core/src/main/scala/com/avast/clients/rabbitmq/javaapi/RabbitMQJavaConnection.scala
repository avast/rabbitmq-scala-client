package com.avast.clients.rabbitmq.javaapi

import java.io.IOException
import java.util.concurrent.{CompletableFuture, ExecutorService, ScheduledExecutorService}

import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.avast.clients.rabbitmq.{ChannelListener, ConnectionListener, ConsumerListener, RabbitMQConnection => ScalaConnection}
import com.avast.metrics.api.Monitor
import com.typesafe.config.Config
import monix.execution.Scheduler

trait RabbitMQJavaConnection extends AutoCloseable {

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[ExecutorService]] used for callbacks.
    */
  def newConsumer(configName: String,
                  monitor: Monitor,
                  ec: ExecutorService,
                  readAction: java.util.function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param ec         [[ExecutorService]] used for sending the message (blocking IO).
    * @param monitor    Monitor for metrics.F
    */
  def newProducer(configName: String, monitor: Monitor, ec: ExecutorService): RabbitMQProducer

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  @throws[IOException]
  def declareExchange(configName: String): Unit

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  @throws[IOException]
  def declareQueue(configName: String): Unit

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindQueue(configName: String): Unit

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindExchange(configName: String): Unit
}

object RabbitMQJavaConnection {

  def newBuilder(config: Config): Builder = {
    new Builder(config)
  }

  //scalastyle:off
  class Builder private[RabbitMQJavaConnection] (config: Config) {
    private var executor: Option[ExecutorService] = None
    private var connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener
    private var channelListener: ChannelListener = DefaultListeners.DefaultChannelListener
    private var consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener
    private var scheduledExecutorService: ScheduledExecutorService = Scheduler.DefaultScheduledExecutor

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

    def withScheduledExecutorService(ses: ScheduledExecutorService): Builder = {
      this.scheduledExecutorService = ses
      this
    }

    def build(): RabbitMQJavaConnection = {
      new RabbitMQJavaConnectionImpl(
        ScalaConnection
          .fromConfig(
            config,
            executor,
            Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
            Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
            Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener),
            scheduledExecutorService = scheduledExecutorService
          )
      )
    }

  }

}
