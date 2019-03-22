package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{DeliveryConverter, RabbitMQConnection => ScalaConnection}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

private class RabbitMQJavaConnectionImpl(scalaConnection: ScalaConnection[Future], initTimeout: Duration)(implicit ec: ExecutionContext)
    extends RabbitMQJavaConnection {

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: ExecutorService,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): DefaultRabbitMQConsumer = {
    implicit val sch: SchedulerService = Scheduler(executor)

    Await.result(
      scalaConnection.newConsumer(configName, ScalaMonitor(monitor))(readAction.asScala).map(new DefaultRabbitMQConsumer(_, initTimeout)),
      initTimeout
    )
  }

  override def newPullConsumer(configName: String, monitor: Monitor, executor: ExecutorService): RabbitMQPullConsumer = {
    implicit val sch: SchedulerService = Scheduler(executor)

    Await.result(
      scalaConnection.newPullConsumer(configName, ScalaMonitor(monitor))(DeliveryConverter.identity, sch).map(new DefaultRabbitMQPullConsumer(_, initTimeout)),
      initTimeout
    )
  }

  override def newProducer(configName: String, monitor: Monitor, executor: ExecutorService): RabbitMQProducer = {
    implicit val sch: SchedulerService = Scheduler(executor)

    Await.result(
      scalaConnection.newProducer[Bytes](configName, ScalaMonitor(monitor)).map(new DefaultRabbitMQProducer(_, initTimeout)),
      initTimeout
    )
  }

  override def declareExchange(configName: String): CompletableFuture[Void] = {
    scalaConnection.declareExchange(configName).map(_ => null: Void).asJava
  }

  override def declareQueue(configName: String): CompletableFuture[Void] = {
    scalaConnection.declareQueue(configName).map(_ => null: Void).asJava
  }

  override def bindQueue(configName: String): CompletableFuture[Void] = {
    scalaConnection.bindQueue(configName).map(_ => null: Void).asJava
  }

  override def bindExchange(configName: String): CompletableFuture[Void] = {
    scalaConnection.bindExchange(configName).map(_ => null: Void).asJava
  }

  override def close(): Unit = Await.result(scalaConnection.close(), initTimeout)

}
