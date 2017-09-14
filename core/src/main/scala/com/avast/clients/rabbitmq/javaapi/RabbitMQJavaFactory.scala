package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function

import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{ConsumerConfig, ProducerConfig, RabbitMQFactory => ScalaFactory}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait RabbitMQJavaFactory extends RabbitMQJavaFactoryManual {

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[Executor]] used for callbacks.
    */
  def newConsumer(configName: String,
                  monitor: Monitor,
                  ec: Executor,
                  readAction: java.util.function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param monitor    Monitor for metrics.F
    */
  def newProducer(configName: String, monitor: Monitor): RabbitMQProducer
}

trait RabbitMQJavaFactoryManual extends AutoCloseable {

  /** Creates new instance of consumer, using passed configuration.
    *
    * @param config     Configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[Executor]] used for callbacks.
    */
  def newConsumer(config: ConsumerConfig, monitor: Monitor, ec: Executor)(
      readAction: java.util.function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer

  /** Creates new instance of producer, using passed configuration.
    *
    * @param config  Configuration of the producer.
    * @param monitor Monitor for metrics.
    */
  def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): Unit
}

private class RabbitMQJavaFactoryImpl(scalaFactory: ScalaFactory) extends RabbitMQJavaFactory {

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: Executor,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    new RabbitMQConsumer(scalaFactory.newConsumer(configName, ScalaMonitor(monitor))(readAction.asScala))
  }

  override def newProducer(configName: String, monitor: Monitor): RabbitMQProducer = {
    new RabbitMQProducer(scalaFactory.newProducer(configName, ScalaMonitor(monitor)))
  }

  override def newConsumer(config: ConsumerConfig, monitor: Monitor, executor: Executor)(
      readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    new RabbitMQConsumer(scalaFactory.newConsumer(config, ScalaMonitor(monitor))(readAction.asScala))
  }

  override def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer = {
    new RabbitMQProducer(scalaFactory.newProducer(config, ScalaMonitor(monitor)))
  }

  override def close(): Unit = scalaFactory.close()
}
