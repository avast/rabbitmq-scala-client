package com.avast.clients.rabbitmq

import scala.concurrent.duration._
import scala.util.Random

class RecoveryDelayHandlersTest extends TestBase {
  test("linear") {
    val rdh = RecoveryDelayHandlers.Linear(10.millis, 42.millis)

    assertResult(10)(rdh.getDelay(0))

    for (_ <- 1 to 200) {
      assertResult(42)(rdh.getDelay(Random.nextInt() + 1))
    }
  }

  test("exponential") {
    val rdh = RecoveryDelayHandlers.Exponential(1.millis, 5.millis, 2.0, 42.millis)

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
