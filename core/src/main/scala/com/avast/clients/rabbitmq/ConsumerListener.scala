package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, Consumer, ShutdownSignalException}

trait ConsumerListener {
  def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit

  @deprecated(since = "6.0.0", message = "This method is never called")
  def onRecoveryStarted(consumer: Consumer, channel: Channel): Unit = ()

  @deprecated(since = "6.0.0", message = "This method is never called")
  def onRecoveryCompleted(consumer: Consumer, channel: Channel): Unit= ()

  @deprecated(since = "6.0.0", message = "This method is never called")
  def onRecoveryFailure(consumer: Consumer, channel: Channel, failure: Throwable): Unit= ()

  def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, sig: ShutdownSignalException): Unit = ()
}

object ConsumerListener {
  final val Default: ConsumerListener = new ConsumerListener {

    override def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit = ()

    override def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, cause: ShutdownSignalException): Unit = ()
  }
}
