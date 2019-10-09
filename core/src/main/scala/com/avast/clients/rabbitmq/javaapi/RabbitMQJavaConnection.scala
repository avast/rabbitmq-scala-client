package com.avast.clients.rabbitmq.javaapi

import java.io.IOException
import java.time.{Duration => JavaDuration}
import java.util.concurrent.{CompletableFuture, ExecutorService}

import cats.effect.{ContextShift, IO, Timer}
import com.avast.clients.rabbitmq.RabbitMQConnection.DefaultListeners
import com.avast.clients.rabbitmq.{ChannelListener, ConnectionListener, ConsumerListener, RabbitMQConnection => ScalaConnection}
import com.avast.metrics.api.Monitor
import com.typesafe.config.Config

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

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

  /** Creates new instance of pull consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param ec         [[ExecutorService]] used for callbacks.
    */
  def newPullConsumer(configName: String, monitor: Monitor, ec: ExecutorService): RabbitMQPullConsumer

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
  def declareExchange(configName: String): CompletableFuture[Void]

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  @throws[IOException]
  def declareQueue(configName: String): CompletableFuture[Void]

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindQueue(configName: String): CompletableFuture[Void]

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindExchange(configName: String): CompletableFuture[Void]
}

object RabbitMQJavaConnection {

  def newBuilder(config: Config, executorService: ExecutorService): Builder = {
    new Builder(config, executorService)
  }

  //scalastyle:off
  class Builder private[RabbitMQJavaConnection] (config: Config, executorService: ExecutorService) {
    private var connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener
    private var channelListener: ChannelListener = DefaultListeners.DefaultChannelListener
    private var consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener
    private var timeout: Duration = 10.seconds

    private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executorService)
    private implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    private implicit val timer: Timer[IO] = IO.timer(ec)


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

    def withTimeout(timeout: JavaDuration): Builder = {
      this.timeout = timeout.toMillis.millis
      this
    }

    def build(): RabbitMQJavaConnection = {

      val (conn, connClose) = Await.result(
        ScalaConnection
          .fromConfig[IO](
            config,
            executorService,
            Option(connectionListener).getOrElse(DefaultListeners.DefaultConnectionListener),
            Option(channelListener).getOrElse(DefaultListeners.DefaultChannelListener),
            Option(consumerListener).getOrElse(DefaultListeners.DefaultConsumerListener)
          )
          .allocated
          .unsafeToFuture(),
        timeout
      )

      new RabbitMQJavaConnectionImpl(conn, timeout) {
        override def close(): Unit = connClose.unsafeRunTimed(timeout)
      }
    }

  }

}
