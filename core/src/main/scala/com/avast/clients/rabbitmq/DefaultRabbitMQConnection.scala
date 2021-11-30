package com.avast.clients.rabbitmq

import cats.effect._
import cats.syntax.flatMap._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

class DefaultRabbitMQConnection[F[_]] private (connection: ServerConnection,
                                               info: RabbitMQConnectionInfo,
                                               republishStrategy: RepublishStrategyConfig,
                                               override val connectionListener: ConnectionListener,
                                               override val channelListener: ChannelListener,
                                               override val consumerListener: ConsumerListener,
                                               blocker: Blocker)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F])
    extends RabbitMQConnection[F]
    with StrictLogging {

  def newChannel(): Resource[F, ServerChannel] = {
    createChannel()
  }

  private val createChannelF: F[ServerChannel] = {
    F.delay {
      try {
        connection.createChannel() match {
          case channel: ServerChannel =>
            logger.debug(s"Created channel: $channel ${channel.hashCode()}")
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
  }

  override def newStreamingConsumer[A: DeliveryConverter](
      consumerConfig: StreamingConsumerConfig,
      monitor: Monitor,
  ): Resource[F, RabbitMQStreamingConsumer[F, A]] = {
    createChannel().flatMap { channel =>
      DefaultRabbitMQClientFactory.StreamingConsumer
        .create[F, A](consumerConfig, channel, createChannelF, info, republishStrategy, blocker, monitor, consumerListener)
        .map(identity[RabbitMQStreamingConsumer[F, A]]) // type inference... :-(
    }
  }

  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = {
    createChannel().map { channel =>
      DefaultRabbitMQClientFactory.Consumer
        .create[F, A](consumerConfig, channel, info, republishStrategy, consumerListener, readAction, blocker, monitor)
    }
  }

  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig,
                                            monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]] = {
    createChannel().map { channel =>
      DefaultRabbitMQClientFactory.PullConsumer
        .create[F, A](pullConsumerConfig, channel, info, republishStrategy, blocker, monitor)
    }
  }

  private def createChannel(): Resource[F, ServerChannel] =
    Resource.make(createChannelF)(channel =>
      F.delay {
        logger.debug(s"Closing channel: $channel ${channel.hashCode()}")
        channel.close()
    })

  override def newProducer[A: ProductConverter](producerConfig: ProducerConfig, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]] = {
    createChannel().map { channel =>
      DefaultRabbitMQClientFactory.Producer
        .create[F, A](producerConfig, channel, info, blocker, monitor)
    }
  }

  override def declareExchange(config: DeclareExchangeConfig): F[Unit] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.declareExchange(config, ch, info)
  }

  override def declareQueue(config: DeclareQueueConfig): F[Unit] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.declareQueue(config, ch)
  }

  override def bindExchange(config: BindExchangeConfig): F[Unit] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.bindExchange(config, ch, info)
  }

  override def bindQueue(config: BindQueueConfig): F[Unit] = withChannel { ch =>
    DefaultRabbitMQClientFactory.Declarations.bindQueue(config, ch, info)
  }

  def withChannel[A](f: ServerChannel => F[A]): F[A] = {
    createChannel().use(f)
  }
}

object DefaultRabbitMQConnection {
  def make[F[_]](connection: ServerConnection,
                 info: RabbitMQConnectionInfo,
                 republishStrategy: RepublishStrategyConfig,
                 connectionListener: ConnectionListener,
                 channelListener: ChannelListener,
                 consumerListener: ConsumerListener,
                 blocker: Blocker)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): F[DefaultRabbitMQConnection[F]] =
    F.delay {
        new DefaultRabbitMQConnection(connection, info, republishStrategy, connectionListener, channelListener, consumerListener, blocker)
      }
      .flatTap { conn =>
        conn.withChannel { channel =>
          setUpRepublishing(republishStrategy, info, channel)
        }
      }

  // prepare exchange for republishing
  private def setUpRepublishing[F[_]: Sync](republishStrategyConfig: RepublishStrategyConfig,
                                            connectionInfo: RabbitMQConnectionInfo,
                                            channel: ServerChannel): F[Unit] = Sync[F].delay {
    republishStrategyConfig match {
      case RepublishStrategyConfig.CustomExchange(exchangeName, exchangeDeclare, _) =>
        if (exchangeDeclare) {
          DefaultRabbitMQClientFactory.declareExchange(
            name = exchangeName,
            `type` = ExchangeType.Direct,
            durable = true,
            autoDelete = false,
            arguments = DeclareArgumentsConfig(),
            channel = channel,
            connectionInfo = connectionInfo
          )
        }

        ()

      case _ => () // no-op
    }
  }
}
