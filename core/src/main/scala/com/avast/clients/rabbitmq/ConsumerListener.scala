package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, Consumer, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

trait ConsumerListener {
  def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): Unit

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
        logger.debug(s"[$consumerName] Shutdown of consumer on channel $channel")
      } else {
        logger.info(s"[$consumerName] Shutdown of consumer on channel $channel", cause)
      }
    }
  }
}
