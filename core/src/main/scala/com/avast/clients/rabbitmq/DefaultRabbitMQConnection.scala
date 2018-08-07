package com.avast.clients.rabbitmq

import cats.effect.Effect
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.ShutdownSignalException
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQConnection[F[_]: Effect](connection: ServerConnection,
                                              info: RabbitMQConnectionInfo,
                                              config: Config,
                                              override val connectionListener: ConnectionListener,
                                              override val channelListener: ChannelListener,
                                              override val consumerListener: ConsumerListener,
                                              blockingScheduler: Scheduler)
    extends RabbitMQConnection[F]
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

  def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): DefaultRabbitMQConsumer[F] = {

    implicit val scheduler = Scheduler(ses, ec)

    addAutoCloseable {
      DefaultRabbitMQClientFactory.Consumer
        .fromConfig[F, A](config.getConfig(configName), createChannel(), info, blockingScheduler, monitor, consumerListener, readAction)
    }
  }

  def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): DefaultRabbitMQPullConsumer[F, A] = {
    implicit val scheduler = Scheduler(ses, ec)

    addAutoCloseable {
      DefaultRabbitMQClientFactory.PullConsumer
        .fromConfig[F, A](config.getConfig(configName), createChannel(), info, blockingScheduler, monitor)
    }
  }

  def newProducer[A: ProductConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): DefaultRabbitMQProducer[F, A] = {
    implicit val scheduler = Scheduler(ses, ec)

    addAutoCloseable {
      DefaultRabbitMQClientFactory.Producer
        .fromConfig[F, A](config.getConfig(configName), createChannel(), info, blockingScheduler, monitor)
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

  protected def addAutoCloseable[A <: AutoCloseable](a: A): A = {
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
    Task {
      val ch = createChannel()

      try {
        f(ch)
          .doOnFinish { e =>
            e.foreach(logger.debug(s"Error while executing action with channel $ch", _))
            Task(ch.close()).executeOn(blockingScheduler)
          }
      } catch {
        case NonFatal(e) =>
          logger.debug(s"Error while executing action with channel $ch", e)

          Task(ch.close())
            .executeOn(blockingScheduler)
            .flatMap(_ => Task.raiseError(e))
      }
    }.flatten // surrounded with Task.apply to catch possible errors when creating the channel
    .asyncBoundary
  }

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): Unit = {
    closeables.foreach(_.close())
    connection.close()
  }

  private def convertFromF[A](task: F[A]): Task[A] = {
    Task.fromEffect(task)
  }

  private def convertToF[A](task: Task[A]): F[A] = {
    task.to[F](Effect[F], blockingScheduler)
  }

}
