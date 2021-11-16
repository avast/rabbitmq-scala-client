package com.avast.clients.rabbitmq

import cats.effect.Sync
import com.rabbitmq.client._
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ConsumerListener[F[_]] {
  def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): F[Unit]

  def onShutdown(consumer: Consumer, channel: Channel, consumerName: String, consumerTag: String, sig: ShutdownSignalException): F[Unit]
}

object ConsumerListener {
  def default[F[_]: Sync]: ConsumerListener[F] = new ConsumerListener[F] {
    private val logger = Slf4jLogger.getLoggerFromClass[F](this.getClass)

    override def onError(consumer: Consumer, consumerName: String, channel: Channel, failure: Throwable): F[Unit] = {
      logger.warn(failure)(s"[$consumerName] Error in consumer on channel $channel")
    }

    override def onShutdown(consumer: Consumer,
                            channel: Channel,
                            consumerName: String,
                            consumerTag: String,
                            cause: ShutdownSignalException): F[Unit] = {
      if (cause.isInitiatedByApplication) {
        logger.debug(s"[$consumerName] Shutdown of consumer on channel $channel")
      } else {
        logger.warn(cause)(s"[$consumerName] Shutdown of consumer on channel $channel")
      }
    }
  }
}
