package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, ShutdownSignalException}

trait ChannelListener {
  def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit

  def onCreate(channel: Channel): Unit

  def onCreateFailure(failure: Throwable): Unit

  def onRecoveryStarted(channel: Channel): Unit

  @deprecated(since = "6.0.0", message = "This method is never called")
  def onRecovery(channel: Channel): Unit = ()

  def onRecoveryCompleted(channel: Channel): Unit

  def onRecoveryFailure(channel: Channel, failure: Throwable): Unit
}

object ChannelListener {
  final val Default: ChannelListener = new ChannelListener {
    override def onCreate(channel: Channel): Unit = ()

    override def onRecoveryCompleted(channel: Channel): Unit = ()

    override def onRecoveryStarted(channel: Channel): Unit = ()

    override def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit = ()

    override def onRecoveryFailure(channel: Channel, failure: Throwable): Unit = ()

    override def onCreateFailure(failure: Throwable): Unit = ()
  }
}
