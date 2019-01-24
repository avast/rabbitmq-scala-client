package com.avast.clients.rabbitmq
import java.time.Duration

import scala.util.Random

class RecoveryDelayHandlersTest extends TestBase {
  test("linear") {
    val rdh = RecoveryDelayHandlers.Linear(Duration.ofMillis(10), Duration.ofMillis(42))

    assertResult(10)(rdh.getDelay(0))

    for (_ <- 1 to 200) {
      assertResult(42)(rdh.getDelay(Random.nextInt() + 1))
    }
  }

  test("exponential") {
    val rdh = RecoveryDelayHandlers.Exponential(Duration.ofMillis(1), Duration.ofMillis(5), 2.0, Duration.ofMillis(42))

    assertResult(1)(rdh.getDelay(0))

    assertResult(5)(rdh.getDelay(1))
    assertResult(10)(rdh.getDelay(2))
    assertResult(20)(rdh.getDelay(3))
    assertResult(40)(rdh.getDelay(4))
    assertResult(42)(rdh.getDelay(5))
    assertResult(42)(rdh.getDelay(6))
    assertResult(42)(rdh.getDelay(7))
  }
}
