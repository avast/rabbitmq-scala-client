package com.avast.clients.rabbitmq

import java.util.concurrent.ScheduledExecutorService

import com.avast.clients.rabbitmq.api.{Delivery, _}
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

class DefaultRabbitMQConnection(connection: ServerConnection,
                                info: RabbitMqFactoryInfo,
                                config: Config,
                                connectionListener: ConnectionListener,
                                channelListener: ChannelListener,
                                consumerListener: ConsumerListener,
                                scheduledExecutorService: ScheduledExecutorService)
    extends RabbitMQConnection
    with StrictLogging {
  // scalastyle:off
  private val closeablesLock = new Object
  private var closeables = Seq[AutoCloseable]()
  // scalastyle:on

  def newChannel(): ServerChannel = addAutoCloseable {
    createChannel()
  }

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

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[ExecutionContext]] used for callbacks.
    */
  def newConsumer(configName: String, monitor: Monitor)(readAction: (Delivery) => Future[DeliveryResult])(
      implicit ec: ExecutionContext): RabbitMQConsumer = addAutoCloseable {
    DefaultRabbitMQClientFactory.Consumer.fromConfig(config.getConfig(configName),
                                                     createChannel(),
                                                     info,
                                                     monitor,
                                                     consumerListener,
                                                     scheduledExecutorService)(readAction)
  }

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param monitor    Monitor for metrics.F
    */
  def newProducer[F[_]: FromTask](configName: String, scheduler: Scheduler, monitor: Monitor): DefaultRabbitMQProducer[F] = {
    addAutoCloseable {
      DefaultRabbitMQClientFactory.Producer.fromConfig(config.getConfig(configName), createChannel(), info, scheduler, monitor)
    }
  }

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareExchange(configName: String): Try[Done] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.declareExchange(config.getConfig(configName), ch, info)
  }

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareQueue(configName: String): Try[Done] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.declareQueue(config.getConfig(configName), ch, info)
  }

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindQueue(configName: String): Try[Done] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.bindQueue(config.getConfig(configName), ch, info)
  }

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindExchange(configName: String): Try[Done] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.bindExchange(config.getConfig(configName), ch, info)
  }

  protected def addAutoCloseable[A <: AutoCloseable](a: A): A = {
    closeablesLock.synchronized {
      closeables = closeables.+:(a)
    }
    a
  }

  def withChannel[A](f: ServerChannel => A): A = {
    val ch = createChannel()

    try {
      f(ch)
    } finally {
      ch.close()
    }
  }

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): Unit = {
    closeables.foreach(_.close())
    connection.close()
  }
}
