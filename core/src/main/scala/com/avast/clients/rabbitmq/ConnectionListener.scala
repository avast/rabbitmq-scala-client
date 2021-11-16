package com.avast.clients.rabbitmq

import cats.effect.Sync
import com.rabbitmq.client.{Connection, ShutdownSignalException}
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ConnectionListener[F[_]] {
  def onCreate(connection: Connection): F[Unit]

  def onCreateFailure(failure: Throwable): F[Unit]

  def onRecoveryStarted(connection: Connection): F[Unit]

  def onRecoveryCompleted(connection: Connection): F[Unit]

  def onRecoveryFailure(connection: Connection, failure: Throwable): F[Unit]

  def onShutdown(connection: Connection, cause: ShutdownSignalException): F[Unit]
}

object ConnectionListener {
  def default[F[_]: Sync]: ConnectionListener[F] = new ConnectionListener[F] {
    private val logger = Slf4jLogger.getLoggerFromClass[F](this.getClass)

    override def onCreate(connection: Connection): F[Unit] = {
      logger.info(s"Connection created: $connection (name ${connection.getClientProvidedName})")
    }

    override def onCreateFailure(failure: Throwable): F[Unit] = {
      logger.warn(failure)(s"Connection NOT created")
    }

    override def onRecoveryStarted(connection: Connection): F[Unit] = {
      logger.info(s"Connection recovery started: $connection (name ${connection.getClientProvidedName})")
    }

    override def onRecoveryCompleted(connection: Connection): F[Unit] = {
      logger.info(s"Connection recovery completed: $connection (name ${connection.getClientProvidedName})")
    }

    override def onRecoveryFailure(connection: Connection, failure: Throwable): F[Unit] = {
      logger.warn(failure)(s"Connection recovery failed: $connection (name ${connection.getClientProvidedName})")
    }

    override def onShutdown(connection: Connection, cause: ShutdownSignalException): F[Unit] = {
      if (cause.isInitiatedByApplication) {
        logger.debug(s"Connection shutdown: $connection (name ${connection.getClientProvidedName})")
      } else {
        logger.warn(cause)(s"Connection shutdown: $connection (name ${connection.getClientProvidedName})")
      }
    }
  }
}
