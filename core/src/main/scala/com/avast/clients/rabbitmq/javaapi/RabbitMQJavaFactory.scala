package com.avast.clients.rabbitmq.javaapi

import java.io.IOException
import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function

import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{
  BindExchange,
  BindQueue,
  ConsumerConfig,
  DeclareExchange,
  DeclareQueue,
  ProducerConfig,
  RabbitMQFactory => ScalaFactory
}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

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

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  @throws[IOException]
  def declareExchange(configName: String): Unit

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  @throws[IOException]
  def declareQueue(configName: String): Unit

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindQueue(configName: String): Unit

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindExchange(configName: String): Unit
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

  /**
    * Declares and additional exchange, using passed configuration.
    */
  @throws[IOException]
  def declareExchange(config: DeclareExchange): Unit

  /**
    * Declares and additional queue, using passed configuration.
    */
  @throws[IOException]
  def declareQueue(config: DeclareQueue): Unit

  /**
    * Binds a queue to an exchange, using passed configuration.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindQueue(config: BindQueue): Unit

  /**
    * Binds an exchange to an another exchange, using passed configuration.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  @throws[IOException]
  def bindExchange(config: BindExchange): Unit

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): Unit
}

private class RabbitMQJavaFactoryImpl(scalaFactory: ScalaFactory) extends RabbitMQJavaFactory {

  import RabbitMQJavaFactoryImpl._

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

  override def declareExchange(configName: String): Unit = {
    scalaFactory.declareExchange(configName).throwFailure()
  }

  override def declareQueue(configName: String): Unit = {
    scalaFactory.declareQueue(configName).throwFailure()
  }

  override def bindQueue(configName: String): Unit = {
    scalaFactory.bindQueue(configName).throwFailure()
  }

  override def bindExchange(configName: String): Unit = {
    scalaFactory.bindExchange(configName).throwFailure()
  }

  override def declareExchange(config: DeclareExchange): Unit = {
    scalaFactory.declareExchange(config).throwFailure()
  }

  override def declareQueue(config: DeclareQueue): Unit = {
    scalaFactory.declareQueue(config).throwFailure()
  }

  override def bindQueue(config: BindQueue): Unit = {
    scalaFactory.bindQueue(config).throwFailure()
  }

  override def bindExchange(config: BindExchange): Unit = {
    scalaFactory.bindExchange(config).throwFailure()
  }

  override def close(): Unit = scalaFactory.close()

}

private object RabbitMQJavaFactoryImpl {

  // this is just to "hack" the ScalaFmt and ScalaStyle ;-) - it's equivalent to `.get`
  implicit class ThrowFailure(val t: Try[_]) {
    def throwFailure(): Unit = t.failed.foreach(throw _)
  }

}
