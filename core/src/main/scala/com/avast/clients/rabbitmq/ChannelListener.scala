package com.avast.clients.rabbitmq

import cats.effect.Sync
import com.rabbitmq.client.impl.recovery.RecoveryAwareChannelN
import com.rabbitmq.client.{Channel, ShutdownSignalException}
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait ChannelListener[F[_]] {
  def onShutdown(cause: ShutdownSignalException, channel: Channel): F[Unit]

  def onCreate(channel: Channel): F[Unit]

  def onCreateFailure(failure: Throwable): F[Unit]

  def onRecoveryStarted(channel: Channel): F[Unit]

  def onRecoveryCompleted(channel: Channel): F[Unit]

  def onRecoveryFailure(channel: Channel, failure: Throwable): F[Unit]
}

object ChannelListener {
  def default[F[_]: Sync]: ChannelListener[F] = new ChannelListener[F] {
    private val logger = Slf4jLogger.getLoggerFromClass[F](this.getClass)

    override def onCreate(channel: Channel): F[Unit] = {
      logger.info(s"Channel created: $channel")
    }

    override def onCreateFailure(failure: Throwable): F[Unit] = {
      logger.warn(failure)(s"Channel was NOT created")
    }

    override def onRecoveryCompleted(channel: Channel): F[Unit] = {
      logger.info(s"Channel recovered: $channel")
    }

    override def onRecoveryStarted(channel: Channel): F[Unit] = {
      logger.debug(s"Channel recovery started: $channel")
    }

    override def onRecoveryFailure(channel: Channel, failure: Throwable): F[Unit] = {
      channel match {
        case ch: RecoveryAwareChannelN if !ch.isOpen =>
          val initByApp = Option(ch.getCloseReason).map(_.isInitiatedByApplication).exists(identity)

          if (initByApp) {
            logger.debug(failure)(s"Channel could not be recovered, because it was manually closed: $channel")
          } else logger.warn(failure)(s"Channel recovery failed: $channel")

        case _ => logger.warn(failure)(s"Channel recovery failed: $channel")
      }
    }

    override def onShutdown(cause: ShutdownSignalException, channel: Channel): F[Unit] = {
      if (cause.isInitiatedByApplication) {
        logger.debug(s"Channel shutdown: $channel")
      } else {
        logger.warn(cause)(s"Channel shutdown: $channel")
      }
    }
  }
}
