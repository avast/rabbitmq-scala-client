package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function

import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.{
  ConsumerConfig,
  ProducerConfig,
  Delivery => ScalaDelivery,
  DeliveryResult => ScalaResult,
  RabbitMQFactory => ScalaFactory
}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
import com.avast.utils2.JavaConverters._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.implicitConversions

trait RabbitMQJavaFactory extends RabbitMQFactoryManual {

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

trait RabbitMQFactoryManual extends AutoCloseable {

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

  private implicit def scalaDeliveryToJava(d: ScalaDelivery): Delivery = {
    new Delivery(d)
  }

  private implicit def scalaReadAction(readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]])(
      implicit ex: Executor,
      ec: ExecutionContext): ScalaDelivery => Future[ScalaResult] = d => readAction(d).asScala.map(ScalaResult.apply)

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: Executor,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    scalaFactory.newConsumer(configName, ScalaMonitor(monitor))(readAction)
  }

  override def newProducer(configName: String, monitor: Monitor): RabbitMQProducer = {
    scalaFactory.newProducer(configName, ScalaMonitor(monitor))
  }

  override def newConsumer(config: ConsumerConfig, monitor: Monitor, executor: Executor)(
      readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    scalaFactory.newConsumer(config, ScalaMonitor(monitor))(readAction)
  }

  override def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer = {
    scalaFactory.newProducer(config, ScalaMonitor(monitor))
  }

  override def close(): Unit = scalaFactory.close()
}
