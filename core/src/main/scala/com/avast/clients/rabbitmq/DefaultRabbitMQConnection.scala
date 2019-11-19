package com.avast.clients.rabbitmq

import cats.effect._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQConnection[F[_]](connection: ServerConnection,
                                      info: RabbitMQConnectionInfo,
                                      override val connectionListener: ConnectionListener,
                                      override val channelListener: ChannelListener,
                                      override val consumerListener: ConsumerListener,
                                      blocker: Blocker)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F])
    extends RabbitMQConnection[F]
    with StrictLogging {

  def newChannel(): Resource[F, ServerChannel] = {
    createChannel()
  }

  private def createChannel(): Resource[F, ServerChannel] =
    Resource.make(F.delay {
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
    })(channel =>
      F.delay {
        logger.debug(s"Closing channel: $channel ${channel.hashCode()}")
        channel.close()
    })

  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = {
    createChannel().map { channel =>
      DefaultRabbitMQClientFactory.Consumer
        .create[F, A](consumerConfig, channel, info, blocker, monitor, consumerListener, readAction)
    }
  }

  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig,
                                            monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]] = {
    createChannel().map { channel =>
      DefaultRabbitMQClientFactory.PullConsumer
        .create[F, A](pullConsumerConfig, channel, info, blocker, monitor)
    }
  }

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
    DefaultRabbitMQClientFactory.Declarations.declareQueue(config, ch, info)
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
