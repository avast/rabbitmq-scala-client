package com.avast.clients.rabbitmq

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class PackageTest extends TestBase {
  test("StreamOps.makeResilient") {
    val c = new AtomicInteger(0)

    val stream = fs2.Stream.eval(Task.now(42))
    val failing = stream.flatMap(_ => fs2.Stream.raiseError[Task](new RuntimeException("my exception")))
    val resilient = failing.makeResilient(3) { _ =>
      Task.delay(c.incrementAndGet())
    }

    assertThrows[RuntimeException] {
      resilient.compile.drain.runSyncUnsafe(1.second)
    }

    assertResult(3)(c.get())
  }
}
