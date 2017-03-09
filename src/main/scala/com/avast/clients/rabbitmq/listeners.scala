package com.avast.clients.rabbitmq

import com.rabbitmq.client.{Channel, Connection, Consumer}
import com.typesafe.scalalogging.StrictLogging
import net.jodah.lyra.event.{ChannelListener, ConnectionListener, ConsumerListener}

//scalastyle:off

class LoggingConnectionListener extends ConnectionListener with StrictLogging {
  override def onCreateFailure(failure: Throwable): Unit = {
    logger.error("Could not create RabbitMQ connection", failure)
  }

  override def onRecoveryCompleted(connection: Connection): Unit = {
    logger.info(s"Recovery of connection '${connection.getClientProvidedName}' was successful")
  }

  override def onRecoveryStarted(connection: Connection): Unit = {
    logger.info(s"Recovery of connection '${connection.getClientProvidedName}' started")
  }

  override def onRecoveryFailure(connection: Connection, failure: Throwable): Unit = {
    logger.warn(s"Recovery of connection '${connection.getClientProvidedName}' has failed", failure)
  }

  override def onRecovery(connection: Connection): Unit = {
    logger.debug(s"Recovery of connection '${connection.getClientProvidedName}' was successful, going to recover channels")
  }

  override def onCreate(connection: Connection): Unit = {
    logger.info(s"Connection '${connection.getClientProvidedName}' was created")
  }
}

class LoggingChannelListener extends ChannelListener with StrictLogging {
  override def onCreateFailure(failure: Throwable): Unit = {
    logger.error("Could not create RabbitMQ channel", failure)
  }

  override def onRecoveryCompleted(channel: Channel): Unit = {
    logger.info(s"Recovery of channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' was successful")
  }

  override def onRecoveryStarted(channel: Channel): Unit = {
    logger.debug(s"Recovery of channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' started")
  }

  override def onRecoveryFailure(channel: Channel, failure: Throwable): Unit = {
    logger.warn(s"Recovery of channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' has failed", failure)
  }

  override def onRecovery(channel: Channel): Unit = {
    logger.debug(s"Recovery of channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' was successful, going to recover related things")
  }

  override def onCreate(channel: Channel): Unit = {
    logger.info(s"Channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' was created")
  }
}

class LoggingConsumerListener extends ConsumerListener with StrictLogging {
  override def onRecoveryCompleted(consumer: Consumer, channel: Channel): Unit = {
    logger.info(s"Recovery of consumer in channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' was successful")
  }

  override def onRecoveryStarted(consumer: Consumer, channel: Channel): Unit = {
    logger.debug(s"Recovery of consumer in channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' started")
  }

  override def onRecoveryFailure(consumer: Consumer, channel: Channel, failure: Throwable): Unit = {
    logger.warn(s"Recovery of consumer in channel no. ${channel.getChannelNumber} in connection '${channel.getConnection.getClientProvidedName}' has failed", failure)
  }
}