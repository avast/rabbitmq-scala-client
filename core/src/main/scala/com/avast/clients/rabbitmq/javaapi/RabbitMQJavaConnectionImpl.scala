package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{RabbitMQProducer => ScalaProducer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{RabbitMQConnection => ScalaConnection}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

private abstract class RabbitMQJavaConnectionImpl(scalaConnection: ScalaConnection[Task], timeout: Duration)(implicit sch: Scheduler)
    extends RabbitMQJavaConnection {

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: ExecutorService,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): DefaultRabbitMQConsumer = {
    implicit val sch: SchedulerService = Scheduler(executor)

    // throw away the consumer itself, it doesn't have any methods now
    val (_, consClose) = Await.result(
      scalaConnection.newConsumer[Bytes](configName, ScalaMonitor(monitor))(readAction.asScala).allocated.runToFuture,
      timeout
    )

    new DefaultRabbitMQConsumer {
      override def close(): Unit = consClose.runSyncUnsafe(timeout)
    }
  }

  override def newPullConsumer(configName: String, monitor: Monitor, executor: ExecutorService): RabbitMQPullConsumer = {
    val (cons, consClose) = Await.result(
      scalaConnection.newPullConsumer[Bytes](configName, ScalaMonitor(monitor)).allocated.runToFuture,
      timeout
    )

    new DefaultRabbitMQPullConsumer(pullConsumerToFuture(cons)) {
      override def close(): Unit = consClose.runSyncUnsafe(timeout)
    }
  }

  override def newProducer(configName: String, monitor: Monitor, executor: ExecutorService): RabbitMQProducer = {
    implicit val sch: SchedulerService = Scheduler(executor)

    val (prod, prodClose) = Await.result(
      scalaConnection.newProducer[Bytes](configName, ScalaMonitor(monitor)).allocated.runToFuture,
      timeout
    )

    val futureProducer: ScalaProducer[Future, Bytes] = FunctorK[ScalaProducer[?[_], Bytes]].mapK(prod)(fkToFuture)

    new DefaultRabbitMQProducer(futureProducer) {
      override def close(): Unit = prodClose.runSyncUnsafe(timeout)
    }
  }

  override def declareExchange(configName: String): CompletableFuture[Void] = {
    scalaConnection.declareExchange(configName).map(_ => null: Void).runToFuture.asJava
  }

  override def declareQueue(configName: String): CompletableFuture[Void] = {
    scalaConnection.declareQueue(configName).map(_ => null: Void).runToFuture.asJava
  }

  override def bindQueue(configName: String): CompletableFuture[Void] = {
    scalaConnection.bindQueue(configName).map(_ => null: Void).runToFuture.asJava
  }

  override def bindExchange(configName: String): CompletableFuture[Void] = {
    scalaConnection.bindExchange(configName).map(_ => null: Void).runToFuture.asJava
  }

}
