package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.{Channel, Consumer, ShutdownSignalException}

trait ConsumerListener extends net.jodah.lyra.event.ConsumerListener {
  def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit
  def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, sig: ShutdownSignalException): Unit = ()
}
