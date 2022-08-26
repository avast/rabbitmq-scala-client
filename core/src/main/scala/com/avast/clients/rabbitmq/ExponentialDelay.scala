package com.avast.clients.rabbitmq

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

class ExponentialDelay(val initialDelay: Duration, val period: Duration, val factor: Double, val maxLength: Duration) {
  private val maxMillis = maxLength.toMillis

  def getExponentialDelay(attempt: Int): FiniteDuration = {
    if (attempt == 0) FiniteDuration(initialDelay._1, initialDelay._2)
    else {
      val millis = math.min(
        maxMillis,
        (period.toMillis * math.pow(factor, attempt - 1)).toLong
        )
      FiniteDuration(millis, TimeUnit.MILLISECONDS)
    }
  }
}
