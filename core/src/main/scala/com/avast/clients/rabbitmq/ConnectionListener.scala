package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Connection, ShutdownSignalException}

trait ConnectionListener {
  def onCreate(connection: Connection): Unit

  def onCreateFailure(failure: Throwable): Unit

  def onRecoveryStarted(connection: Connection): Unit

  @deprecated(since = "6.0.0", message = "This method is never called")
  def onRecovery(connection: Connection): Unit = ()

  def onRecoveryCompleted(connection: Connection): Unit

  def onRecoveryFailure(connection: Connection, failure: Throwable): Unit

  def onShutdown(connection: Connection, cause: ShutdownSignalException): Unit
}

object ConnectionListener {
  final val Default: ConnectionListener = new ConnectionListener {
    override def onCreate(connection: Connection): Unit = ()

    override def onCreateFailure(failure: Throwable): Unit = ()

    override def onRecoveryStarted(connection: Connection): Unit = ()

    override def onRecoveryCompleted(connection: Connection): Unit = ()

    override def onRecoveryFailure(connection: Connection, failure: Throwable): Unit = ()

    override def onShutdown(connection: Connection, cause: ShutdownSignalException): Unit = ()
  }
}
