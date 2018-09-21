package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, Consumer, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

trait ConsumerListener {
  @deprecated("6.0.0", "Use onError(Consumer, String, Channel, Throwable) instead. This method is never called.")
  def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit = ()

  def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): Unit

  @deprecated("6.0.0", "This method is never called")
  def onRecoveryStarted(consumer: Consumer, channel: Channel): Unit = ()

  @deprecated("6.0.0", "This method is never called")
  def onRecoveryCompleted(consumer: Consumer, channel: Channel): Unit = ()

  @deprecated("6.0.0", "This method is never called")
  def onRecoveryFailure(consumer: Consumer, channel: Channel, failure: Throwable): Unit = ()

  @deprecated("6.0.0", "Use onShutdown(Consumer, Channel, String, String, ShutdownSignalException) instead. This method is never called.")
  def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, sig: ShutdownSignalException): Unit = ()

  def onShutdown(consumer: Consumer, channel: Channel, consumerName: String, consumerTag: String, sig: ShutdownSignalException): Unit
}

object ConsumerListener {
  final val Default: ConsumerListener = new ConsumerListener with StrictLogging {

    override def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): Unit = {
      logger.warn(s"[$consumerName] Error in consumer on channel $channel", failure)
    }

    override def onShutdown(consumer: Consumer,
                            channel: Channel,
                            consumerName: String,
                            consumerTag: String,
                            cause: ShutdownSignalException): Unit = {
      if (cause.isInitiatedByApplication) {
        logger.info(s"[$consumerName] Shutdown of consumer on channel $channel")
      } else {
        logger.warn(s"[$consumerName] Shutdown of consumer on channel $channel", cause)
      }
    }
  }
}
