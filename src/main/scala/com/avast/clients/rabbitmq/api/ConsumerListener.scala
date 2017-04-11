package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.{Channel, Consumer}

trait ConsumerListener extends net.jodah.lyra.event.ConsumerListener {
  def onError(consumer: Consumer, channel: Channel, failure: Throwable): Unit
}
