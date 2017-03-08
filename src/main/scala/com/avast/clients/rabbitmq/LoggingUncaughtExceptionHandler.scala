package com.avast.clients.rabbitmq

import com.rabbitmq.client._
import com.typesafe.scalalogging.LazyLogging

class LoggingUncaughtExceptionHandler extends ExceptionHandler with LazyLogging {
  override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = {
    logger.error("Unexpected connection driver exception, closing the connection", exception)
    conn.abort()
  }

  override def handleConsumerException(channel: Channel,
                                       exception: Throwable,
                                       consumer: Consumer,
                                       consumerTag: String,
                                       methodName: String): Unit = {
    logger.warn(s"Error in consumer $consumerTag (while calling $methodName)", exception)
  }

  override def handleBlockedListenerException(connection: Connection, exception: Throwable): Unit = {
    logger.error("Unexpected blocked listener exception, closing the connection", exception)
    connection.abort()
  }

  override def handleFlowListenerException(channel: Channel, exception: Throwable): Unit = {
    logger.error("Unexpected flow listener exception", exception)
  }

  override def handleReturnListenerException(channel: Channel, exception: Throwable): Unit = {
    logger.error("Unexpected return listener exception", exception)
  }

  override def handleConfirmListenerException(channel: Channel, exception: Throwable): Unit = {
    logger.error("Unexpected confirm listener exception", exception)
  }

  override def handleTopologyRecoveryException(conn: Connection, ch: Channel, exception: TopologyRecoveryException): Unit = {
    logger.warn(s"Error in topology recovery to ${conn.getAddress}", exception)
  }

  override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = {
    logger.warn(s"Error in channel recovery (of connection to ${ch.getConnection})", exception)
  }

  override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = {
    logger.warn(s"Error in connection recovery to ${conn.getAddress}", exception)
  }
}
