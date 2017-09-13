package com.avast.clients.rabbitmq

import java.util.concurrent.ScheduledExecutorService

import com.avast.clients.rabbitmq.RabbitMQFactory.{ServerChannel, ServerConnection}
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

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
    val channel = connection.createChannel()
    channel.addShutdownListener((cause: ShutdownSignalException) => channelListener.onShutdown(cause, channel))
    channel
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

  private def addAutoCloseable[A <: AutoCloseable](a: A): A = {
    closeablesLock.synchronized {
      closeables = closeables.+:(a)
    }
    a
  }

  override def close(): Unit = {
    closeables.foreach(_.close())
  }
}
