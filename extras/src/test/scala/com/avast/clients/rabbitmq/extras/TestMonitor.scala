package com.avast.clients.rabbitmq.extras

import com.avast.metrics.api.Naming
import com.avast.metrics.dropwizard.MetricsMonitor
import com.avast.metrics.scalaapi._
import com.codahale.metrics.MetricRegistry

import scala.jdk.CollectionConverters._

class TestMonitor extends Monitor {

  private val metricsRegistry = new MetricRegistry
  val underlying: Monitor = Monitor(new MetricsMonitor(metricsRegistry, Naming.defaultNaming()))

  override def named(name: String): Monitor = underlying.named(name)
  override def named(name1: String, name2: String, restOfNames: String*): Monitor = underlying.named(name1, name2, restOfNames: _*)
  override def meter(name: String): Meter = underlying.meter(name)
  override def counter(name: String): Counter = underlying.counter(name)
  override def histogram(name: String): Histogram = underlying.histogram(name)
  override def timer(name: String): Timer = underlying.timer(name)
  override def timerPair(name: String): TimerPair = underlying.timerPair(name)
  override def gauge[A](name: String)(gauge: () => A): Gauge[A] = underlying.gauge(name)(gauge)
  override def gauge[A](name: String, replaceExisting: Boolean)(gauge: () => A): Gauge[A] = underlying.gauge(name, replaceExisting)(gauge)

  override def asJava: com.avast.metrics.api.Monitor = underlying.asJava
  override def getName: String = underlying.getName

  override def close(): Unit = underlying.close()

  val registry: Registry = new Registry(metricsRegistry)
}

class Registry(registry: MetricRegistry) {
  def meterCount(path: String): Long = registry.getMeters.asScala(path.replace('.', '/')).getCount
  def timerCount(path: String): Long = registry.getTimers.asScala(path.replace('.', '/')).getCount
  def timerPairCountSuccesses(path: String): Long = registry.getTimers.asScala(path.replace('.', '/') + "Successes").getCount
  def timerPairCountFailures(path: String): Long = registry.getTimers.asScala(path.replace('.', '/') + "Failures").getCount
}
