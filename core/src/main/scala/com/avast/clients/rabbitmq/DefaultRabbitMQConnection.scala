package com.avast.clients.rabbitmq

import cats.effect.Effect
import cats.syntax.all._
import com.avast.clients.rabbitmq.DefaultRabbitMQClientFactory.FakeConfigRootName
import com.avast.clients.rabbitmq.api.{FAutoCloseable, RabbitMQConsumer, RabbitMQProducer, RabbitMQPullConsumer}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
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
  // scalastyle:off
  private val closeablesLock = new Object
  private var closeables = Seq[FAutoCloseable[F]]()
  // scalastyle:on

  def newChannel(): F[ServerChannel] = {
    createChannel().flatMap { channel =>
      addAutoCloseable {
        F.delay { (() => F.delay(channel.close())): FAutoCloseable[F] }
      }.map(_ => channel)
    }
  }

  private def createChannel(): F[ServerChannel] = F.delay {
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

  def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): F[RabbitMQConsumer[F]] = {
    addAutoCloseable {
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
  }

  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): F[RabbitMQConsumer[F]] = {
    addAutoCloseable {
      createChannel().map { channel =>
        implicit val scheduler = Scheduler(ses, ec)

        DefaultRabbitMQClientFactory.Consumer
          .create[F, A](consumerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor, consumerListener, readAction)
      }
    }
  }

  def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): F[RabbitMQPullConsumer[F, A]] = {
    addAutoCloseable {
      createChannel().map { channel =>
        implicit val scheduler = Scheduler(ses, ec)

        DefaultRabbitMQClientFactory.PullConsumer
          .fromConfig[F, A](config.getConfig(configName), s"$FakeConfigRootName.$configName", channel, info, blockingScheduler, monitor)
      }
    }
  }

  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig, monitor: Monitor)(
      implicit ec: ExecutionContext): F[RabbitMQPullConsumer[F, A]] = {
    addAutoCloseable {
      createChannel().map { channel =>
        implicit val scheduler = Scheduler(ses, ec)

        DefaultRabbitMQClientFactory.PullConsumer
          .create[F, A](pullConsumerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor)
      }
    }
  }

  def newProducer[A: ProductConverter](configName: String, monitor: Monitor)(implicit ec: ExecutionContext): F[RabbitMQProducer[F, A]] = {
    addAutoCloseable {
      createChannel().map { channel =>
        implicit val scheduler = Scheduler(ses, ec)

        DefaultRabbitMQClientFactory.Producer
          .fromConfig[F, A](config.getConfig(configName), s"$FakeConfigRootName.$configName", channel, info, blockingScheduler, monitor)
      }
    }
  }

  override def newProducer[A: ProductConverter](producerConfig: ProducerConfig, monitor: Monitor)(
      implicit ec: ExecutionContext): F[RabbitMQProducer[F, A]] = {
    addAutoCloseable {
      createChannel().map { channel =>
        implicit val scheduler = Scheduler(ses, ec)

        DefaultRabbitMQClientFactory.Producer
          .create[F, A](producerConfig, "_manually_provided_", channel, info, blockingScheduler, monitor)
      }
    }
  }

  def declareExchange(configName: String): F[Unit] = convertToF {
    taskWithChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.declareExchange(config.getConfig(configName), ch, info)
    }
  }

  def declareQueue(configName: String): F[Unit] = convertToF {
    taskWithChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.declareQueue(config.getConfig(configName), ch, info)
    }
  }

  def bindQueue(configName: String): F[Unit] = convertToF {
    taskWithChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.bindQueue(config.getConfig(configName), ch, info)
    }
  }

  def bindExchange(configName: String): F[Unit] = convertToF {
    taskWithChannel { ch =>
      DefaultRabbitMQClientFactory.Declarations.bindExchange(config.getConfig(configName), ch, info)
    }
  }

  protected def addAutoCloseable[A <: FAutoCloseable[F]](a: F[A]): F[A] = a.map { a =>
    closeablesLock.synchronized {
      closeables = a +: closeables
    }
    a
  }

  def withChannel[A](f: ServerChannel => F[A]): F[A] = convertToF {
    taskWithChannel { ch =>
      convertFromF(f(ch))
    }
  }

  private def taskWithChannel[A](f: ServerChannel => Task[A]): Task[A] = {
    Task
      .fromEffect(createChannel())
      .map { channel =>
        try {
          f(channel).doOnFinish { e =>
            e.foreach(logger.debug(s"Error while executing action with channel $channel", _))
            Task(channel.close()).executeOn(blockingScheduler)
          }
        } catch {
          case NonFatal(e) =>
            logger.debug(s"Error while executing action with channel $channel", e)

            Task(channel.close()).executeOn(blockingScheduler).flatMap(_ => Task.raiseError(e))
        }
      }
      .flatten // surrounded with Task.apply to catch possible errors when creating the channel
      .asyncBoundary
  }

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): F[Unit] = F.delay {
    closeables.foreach { cl =>
      try {
        cl.close()
      } catch {
        case NonFatal(e) => logger.error(s"Could not close some resource: $cl", e)
      }
    }
    connection.close()
  }

  private def convertFromF[A](task: F[A]): Task[A] = {
    Task.fromEffect(task)
  }

  private def convertToF[A](task: Task[A]): F[A] = {
    task.to[F](Effect[F], blockingScheduler)
  }

}
