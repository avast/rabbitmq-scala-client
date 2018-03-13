package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function

import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{DefaultRabbitMQConnection => ScalaConnection}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

private class RabbitMQJavaConnectionImpl(ScalaConnection: ScalaConnection) extends RabbitMQJavaConnection {

  import RabbitMQJavaConnectionImpl._

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: ExecutorService,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    new RabbitMQConsumer(ScalaConnection.newConsumer(configName, ScalaMonitor(monitor))(readAction.asScala))
  }

  override def newProducer(configName: String, monitor: Monitor, executor: ExecutorService): RabbitMQProducer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    import com.avast.clients.rabbitmq.fkTry
    new RabbitMQProducer(ScalaConnection.newProducer[Try](configName, Scheduler(executor), ScalaMonitor(monitor)))
  }

  override def declareExchange(configName: String): Unit = {
    ScalaConnection.declareExchange(configName).throwFailure()
  }

  override def declareQueue(configName: String): Unit = {
    ScalaConnection.declareQueue(configName).throwFailure()
  }

  override def bindQueue(configName: String): Unit = {
    ScalaConnection.bindQueue(configName).throwFailure()
  }

  override def bindExchange(configName: String): Unit = {
    ScalaConnection.bindExchange(configName).throwFailure()
  }

  override def close(): Unit = ScalaConnection.close()

}

private object RabbitMQJavaConnectionImpl {

  // this is just to "hack" the ScalaFmt and ScalaStyle ;-) - it's equivalent to `.get`
  implicit class ThrowFailure(val t: Try[_]) {
    def throwFailure(): Unit = t.failed.foreach(throw _)
  }

}
