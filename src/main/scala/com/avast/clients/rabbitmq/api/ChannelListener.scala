package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.{Channel, ShutdownSignalException}

trait ChannelListener extends net.jodah.lyra.event.ChannelListener {
  def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit
}
