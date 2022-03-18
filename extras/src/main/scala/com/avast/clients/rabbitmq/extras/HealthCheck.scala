package com.avast.clients.rabbitmq.extras

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits.catsSyntaxFlatMapOps
import com.avast.clients.rabbitmq.extras.HealthCheckStatus.{Failure, Ok}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.{ChannelListener, ConnectionListener, ConsumerListener}
import com.rabbitmq.client._

sealed trait HealthCheckStatus

object HealthCheckStatus {

  case object Ok extends HealthCheckStatus

  case class Failure(msg: String, f: Throwable) extends HealthCheckStatus

}

/*
 * The library is not able to recover from all failures so it provides this class that indicates if the application
 * is OK or not - then it should be restarted.
 * To use this class, simply pass the `rabbitExceptionHandler` as listener when constructing the RabbitMQ classes.
 * Then you can call `getStatus` method.
 */
class HealthCheck[F[_]: Sync] {
  private val F: Sync[F] = Sync[F] // scalastyle:ignore

  private val logger = ImplicitContextLogger.createLogger[F, HealthCheck[F]]

  private val ref = Ref.unsafe[F, HealthCheckStatus](Ok)

  def getStatus: F[HealthCheckStatus] = ref.get

  def fail(e: Throwable): F[Unit] = {
    val s = Failure(e.getClass.getName + ": " + e.getMessage, e)
    ref.set(s) >>
      logger.plainWarn(e)(s"Failing HealthCheck with '${s.msg}'")
  }

  def rabbitExceptionHandler: ConnectionListener[F] with ChannelListener[F] with ConsumerListener[F] =
    new ConnectionListener[F] with ChannelListener[F] with ConsumerListener[F] {
      override def onRecoveryCompleted(connection: Connection): F[Unit] = F.unit

      override def onRecoveryStarted(connection: Connection): F[Unit] = F.unit

      override def onRecoveryFailure(connection: Connection, failure: Throwable): F[Unit] = fail(failure)

      override def onCreate(connection: Connection): F[Unit] = F.unit

      override def onCreateFailure(failure: Throwable): F[Unit] = fail(failure)

      override def onRecoveryCompleted(channel: Channel): F[Unit] = F.unit

      override def onRecoveryStarted(channel: Channel): F[Unit] = F.unit

      override def onRecoveryFailure(channel: Channel, failure: Throwable): F[Unit] = fail(failure)

      override def onCreate(channel: Channel): F[Unit] = F.unit

      override def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): F[Unit] = fail(failure)

      override def onShutdown(consumer: Consumer,
                              channel: Channel,
                              consumerName: String,
                              consumerTag: String,
                              cause: ShutdownSignalException): F[Unit] = fail(cause)

      override def onShutdown(connection: Connection, cause: ShutdownSignalException): F[Unit] = fail(cause)

      override def onShutdown(cause: ShutdownSignalException, channel: Channel): F[Unit] = fail(cause)
    }

}
