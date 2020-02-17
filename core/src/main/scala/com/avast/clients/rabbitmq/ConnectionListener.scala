package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Connection, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

trait ConnectionListener {
  def onCreate(connection: Connection): Unit

  def onCreateFailure(failure: Throwable): Unit

  def onRecoveryStarted(connection: Connection): Unit

  def onRecoveryCompleted(connection: Connection): Unit

  def onRecoveryFailure(connection: Connection, failure: Throwable): Unit

  def onShutdown(connection: Connection, cause: ShutdownSignalException): Unit
}

object ConnectionListener {
  final val Default: ConnectionListener = new ConnectionListener with StrictLogging {
    override def onCreate(connection: Connection): Unit = {
      logger.info(s"Connection created: $connection (name ${connection.getClientProvidedName})")
    }

    override def onCreateFailure(failure: Throwable): Unit = {
      logger.warn(s"Connection NOT created", failure)
    }

    override def onRecoveryStarted(connection: Connection): Unit = {
      logger.info(s"Connection recovery started: $connection (name ${connection.getClientProvidedName})")
    }

    override def onRecoveryCompleted(connection: Connection): Unit = {
      logger.info(s"Connection recovery completed: $connection (name ${connection.getClientProvidedName})")
    }

    override def onRecoveryFailure(connection: Connection, failure: Throwable): Unit = {
      logger.warn(s"Connection recovery failed: $connection (name ${connection.getClientProvidedName})", failure)
    }

    override def onShutdown(connection: Connection, cause: ShutdownSignalException): Unit = {
      if (cause.isInitiatedByApplication) {
        logger.debug(s"Connection shutdown: $connection (name ${connection.getClientProvidedName})")
      } else {
        logger.warn(s"Connection shutdown: $connection (name ${connection.getClientProvidedName})", cause)
      }
    }
  }
}
