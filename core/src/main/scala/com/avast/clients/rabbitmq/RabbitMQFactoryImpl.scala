package com.avast.clients.rabbitmq

import java.util.concurrent.ScheduledExecutorService

import com.avast.clients.rabbitmq.RabbitMQFactory.{ServerChannel, ServerConnection}
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

private[rabbitmq] class RabbitMQFactoryImpl(connection: ServerConnection,
                                            info: RabbitMqFactoryInfo,
                                            config: Config,
                                            connectionListener: ConnectionListener,
                                            channelListener: ChannelListener,
                                            consumerListener: ConsumerListener,
                                            scheduledExecutorService: ScheduledExecutorService)
    extends RabbitMQFactory {
  // scalastyle:off
  private val closeablesLock = new Object
  private var closeables = Seq[AutoCloseable]()
  // scalastyle:on

  private def createChannel(): ServerChannel = {
    try {
      connection.createChannel() match {
        case channel: ServerChannel =>
          channel.addShutdownListener((cause: ShutdownSignalException) => channelListener.onShutdown(cause, channel))
          channelListener.onCreate(channel)
          channel

        // since the connection is `Recoverable`, the channel should always be `Recoverable` too (based on docs), so the exception will never be thrown
        case _ => throw new IllegalStateException(s"Required Recoverable Channel")
      }
    } catch {
      case NonFatal(e) =>
        channelListener.onCreateFailure(e)
        throw e
    }
  }

  override def newConsumer(configName: String, monitor: Monitor)(readAction: (Delivery) => Future[DeliveryResult])(
      implicit ec: ExecutionContext): RabbitMQConsumer = addAutoCloseable {
    RabbitMQClientFactory.Consumer.fromConfig(config.getConfig(configName),
                                              createChannel(),
                                              info,
                                              monitor,
                                              consumerListener,
                                              scheduledExecutorService)(readAction)
  }

  override def newConsumer(config: ConsumerConfig, monitor: Monitor)(readAction: (Delivery) => Future[DeliveryResult])(
      implicit ec: ExecutionContext): RabbitMQConsumer = addAutoCloseable {
    RabbitMQClientFactory.Consumer.create(config, createChannel(), info, monitor, consumerListener, scheduledExecutorService)(readAction)
  }

  override def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer = addAutoCloseable {
    RabbitMQClientFactory.Producer.create(config, createChannel(), info, monitor)
  }

  override def newProducer(configName: String, monitor: Monitor): RabbitMQProducer = addAutoCloseable {
    RabbitMQClientFactory.Producer.fromConfig(config.getConfig(configName), createChannel(), info, monitor)
  }

  override def declareExchange(configName: String): Try[Done] = {
    RabbitMQClientFactory.Declarations.declareExchange(config.getConfig(configName), createChannel(), info)
  }

  override def declareQueue(configName: String): Try[Done] = {
    RabbitMQClientFactory.Declarations.declareQueue(config.getConfig(configName), createChannel(), info)
  }

  override def bindQueue(configName: String): Try[Done] = {
    RabbitMQClientFactory.Declarations.bindQueue(config.getConfig(configName), createChannel(), info)
  }

  override def declareExchange(config: DeclareExchange): Try[Done] = {
    RabbitMQClientFactory.Declarations.declareExchange(config, createChannel(), info)
  }

  override def declareQueue(config: DeclareQueue): Try[Done] = {
    RabbitMQClientFactory.Declarations.declareQueue(config, createChannel(), info)
  }

  override def bindQueue(config: BindQueue): Try[Done] = {
    RabbitMQClientFactory.Declarations.bindQueue(config, createChannel(), info)
  }

  override def bindExchange(configName: String): Try[Done] = {
    RabbitMQClientFactory.Declarations.bindExchange(config.getConfig(configName), createChannel(), info)
  }

  override def bindExchange(config: BindExchange): Try[Done] = {
    RabbitMQClientFactory.Declarations.bindExchange(config, createChannel(), info)
  }

  private def addAutoCloseable[A <: AutoCloseable](a: A): A = {
    closeablesLock.synchronized {
      closeables = closeables.+:(a)
    }
    a
  }

  override def close(): Unit = {
    closeables.foreach(_.close())
    connection.close()
  }
}
