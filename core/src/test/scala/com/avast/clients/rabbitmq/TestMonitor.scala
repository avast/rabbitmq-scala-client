package com.avast.clients.rabbitmq

import cats.effect.Sync
import com.avast.metrics.api.Naming
import com.avast.metrics.dropwizard.MetricsMonitor
import com.avast.metrics.scalaapi
import com.avast.metrics.scalaeffectapi._
import com.codahale.metrics.MetricRegistry

import scala.jdk.CollectionConverters._

class TestMonitor[F[_]: Sync] extends Monitor[F] {

  private val metricsRegistry = new MetricRegistry
  val underlying: Monitor[F] = Monitor.wrapJava[F](new MetricsMonitor(metricsRegistry, Naming.defaultNaming()))

  override def named(name: String): Monitor[F] = underlying.named(name)
  override def named(name1: String, name2: String, restOfNames: String*): Monitor[F] = underlying.named(name1, name2, restOfNames: _*)
  override def meter(name: String): Meter[F] = underlying.meter(name)
  override def counter(name: String): Counter[F] = underlying.counter(name)
  override def histogram(name: String): Histogram[F] = underlying.histogram(name)
  override def timer(name: String): Timer[F] = underlying.timer(name)
  override def timerPair(name: String): TimerPair[F] = underlying.timerPair(name)
  override def gauge: GaugeFactory[F] = underlying.gauge
  override def asPlainScala: scalaapi.Monitor = underlying.asPlainScala
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
  def gaugeLongValue(path: String): Long = registry.getGauges.asScala(path.replace('.', '/')).getValue.asInstanceOf[Long]
}
