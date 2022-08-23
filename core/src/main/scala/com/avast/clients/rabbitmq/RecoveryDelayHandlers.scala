package com.avast.clients.rabbitmq

import com.rabbitmq.client.RecoveryDelayHandler

import scala.concurrent.duration._

// Delay intervals should avoid values that are too low (lower than 2 seconds).
// https://www.rabbitmq.com/api-guide.html#automatic-recovery-limitations
object RecoveryDelayHandlers {
  case class Linear(initialDelay: Duration = 5.second, period: Duration = 5.seconds) extends RecoveryDelayHandler {
    override def getDelay(recoveryAttempts: Int): Long = {
      if (recoveryAttempts == 0) initialDelay.toMillis else period.toMillis
    }
  }

  case class Exponential(override val initialDelay: Duration = 5.second,
                         override val period: Duration = 5.seconds,
                         override val factor: Double = 2.0,
                         override val maxLength: Duration = 32.seconds)
      extends ExponentialDelay(initialDelay, period, factor, maxLength) with RecoveryDelayHandler {
    override def getDelay(recoveryAttempts: Int): Long = {
      getExponentialDelay(recoveryAttempts).toMillis
    }
  }
}
