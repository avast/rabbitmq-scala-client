package com.avast.clients.rabbitmq

import cats.effect.{Effect, Resource}
import com.avast.clients.rabbitmq.DefaultRabbitMQClientFactory.FakeConfigRootName
import com.avast.clients.rabbitmq.api.{RabbitMQConsumer, RabbitMQProducer, RabbitMQPullConsumer}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQConnection[F[_]](connection: ServerConnection,
                                      info: RabbitMQConnectionInfo,
                                      config: Config,
                                      override val connectionListener: ConnectionListener,
                                      override val channelListener: ChannelListener,
                                      override val consumerListener: ConsumerListener,
                                      blockingScheduler: Scheduler)(implicit F: Effect[F])
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

  def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): Resource[F, RabbitMQConsumer[F]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.Consumer
        .fromConfig[F, A](config.getConfig(configName),
                          s"$FakeConfigRootName.$configName",
                          channel,
                          info,
                          blockingScheduler,
                          monitor,
                          consumerListener,
                          readAction)
    }
  }

  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): Resource[F, RabbitMQConsumer[F]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.Consumer
        .create[F, A](consumerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor, consumerListener, readAction)
    }
  }

  def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): Resource[F, RabbitMQPullConsumer[F, A]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.PullConsumer
        .fromConfig[F, A](config.getConfig(configName), s"$FakeConfigRootName.$configName", channel, info, blockingScheduler, monitor)
    }
  }

  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig, monitor: Monitor)(
      implicit ec: ExecutionContext): Resource[F, RabbitMQPullConsumer[F, A]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.PullConsumer
        .create[F, A](pullConsumerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor)
    }
  }

  def newProducer[A: ProductConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): Resource[F, RabbitMQProducer[F, A]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.Producer
        .fromConfig[F, A](config.getConfig(configName), s"$FakeConfigRootName.$configName", channel, info, blockingScheduler, monitor)
    }
  }

  override def newProducer[A: ProductConverter](producerConfig: ProducerConfig, monitor: Monitor)(
      implicit ec: ExecutionContext): Resource[F, RabbitMQProducer[F, A]] = {
    createChannel().map { channel =>
      implicit val scheduler = Scheduler(ses, ec)

      DefaultRabbitMQClientFactory.Producer
        .create[F, A](producerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor)
    }
  }

  def declareExchange(configName: String): F[Unit] = {
    withChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.declareExchange(config.getConfig(configName), ch, info)
    }
  }

  def declareQueue(configName: String): F[Unit] = {
    withChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.declareQueue(config.getConfig(configName), ch, info)
    }
  }

  def bindQueue(configName: String): F[Unit] = {
    withChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.bindQueue(config.getConfig(configName), ch, info)
    }
  }

  def bindExchange(configName: String): F[Unit] = {
    withChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.bindExchange(config.getConfig(configName), ch, info)
    }
  }

  def withChannel[A](f: ServerChannel => F[A]): F[A] = {
    createChannel().use(f)
  }

  /** Closes this factory and all created consumers and producers.
    */
  private[rabbitmq] def close(): F[Unit] = F.delay {
    connection.close()
  }
}
