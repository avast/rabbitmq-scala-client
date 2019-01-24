package com.avast.clients.rabbitmq
import java.time.Duration

import com.rabbitmq.client.RecoveryDelayHandler

private[rabbitmq] object RecoveryDelayHandlers {
  case class Linear(delay: Duration, period: Duration) extends RecoveryDelayHandler {
    override def getDelay(recoveryAttempts: Int): Long = {
      if (recoveryAttempts == 0) delay.toMillis else period.toMillis
    }
  }

  case class Exponential(delay: Duration, period: Duration, factor: Double, maxLength: Duration) extends RecoveryDelayHandler {
    private val maxMillis = maxLength.toMillis

    override def getDelay(recoveryAttempts: Int): Long = {
      if (recoveryAttempts == 0) delay.toMillis
      else {
        math.min(
          maxMillis,
          (period.toMillis * math.pow(factor, recoveryAttempts - 1)).toLong
        )
      }
    }
  }
}
