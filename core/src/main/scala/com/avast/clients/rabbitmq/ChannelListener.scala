package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

trait ChannelListener {
  def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit

  def onCreate(channel: Channel): Unit

  def onCreateFailure(failure: Throwable): Unit

  def onRecoveryStarted(channel: Channel): Unit

  def onRecoveryCompleted(channel: Channel): Unit

  def onRecoveryFailure(channel: Channel, failure: Throwable): Unit
}

object ChannelListener {
  final val Default: ChannelListener = new ChannelListener with StrictLogging {
    override def onCreate(channel: Channel): Unit = {
      logger.info(s"Channel created: $channel")
    }

    override def onCreateFailure(failure: Throwable): Unit = {
      logger.warn(s"Channel was NOT created", failure)
    }

    override def onRecoveryCompleted(channel: Channel): Unit = {
      logger.info(s"Channel recovered: $channel")
    }

    override def onRecoveryStarted(channel: Channel): Unit = {
      logger.debug(s"Channel recovery started: $channel")
    }

    override def onRecoveryFailure(channel: Channel, failure: Throwable): Unit = {
      logger.warn(s"Channel recovery failed: $channel", failure)
    }

    override def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit = {
      if (cause.isInitiatedByApplication) {
        logger.debug(s"Channel shutdown: $channel")
      } else {
        logger.info(s"Channel shutdown: $channel", cause)
      }
    }
  }
}
