package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.{function, Date}

import com.avast.clients.rabbitmq.api.{Delivery => ScalaDelivery, DeliveryResult => ScalaResult, MessageProperties => ScalaProperties}
import com.avast.clients.rabbitmq.{ConsumerConfig, ProducerConfig, api, RabbitMQFactory => ScalaFactory}
import com.avast.metrics.api.Monitor
import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
import com.avast.utils2.JavaConverters._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.implicitConversions

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

  private implicit def messagePropertiesToAmqp(messageProperties: ScalaProperties): MessageProperties = {
    val builder = MessageProperties.newBuilder()

    builder.headers(messageProperties.headers.asJava)
    messageProperties.contentType.foreach(builder.contentType)
    messageProperties.contentEncoding.foreach(builder.contentEncoding)
    messageProperties.deliveryMode.foreach(builder.deliveryMode)
    messageProperties.priority.foreach(builder.priority)
    messageProperties.correlationId.foreach(builder.correlationId)
    messageProperties.replyTo.foreach(builder.replyTo)
    messageProperties.expiration.foreach(builder.expiration)
    messageProperties.messageId.foreach(builder.messageId)
    messageProperties.timestamp.map(i => new Date(i.toEpochMilli)).foreach(builder.timestamp)
    messageProperties.`type`.foreach(builder.`type`)
    messageProperties.userId.foreach(builder.userId)
    messageProperties.appId.foreach(builder.appId)
    messageProperties.clusterId.foreach(builder.clusterId)

    builder.build()
  }

  private implicit def scalaDeliveryToJava(d: ScalaDelivery): Delivery = {
    new Delivery(d.routingKey, d.body, d.properties)
  }

  private implicit def scalaReadAction(readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]])(
      implicit ex: Executor,
      ec: ExecutionContext): ScalaDelivery => Future[api.DeliveryResult] = d => readAction(d).asScala.map(ScalaResult.apply)

  override def newConsumer(configName: String,
                           monitor: Monitor,
                           executor: Executor,
                           readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    new RabbitMQConsumer(scalaFactory.newConsumer(configName, ScalaMonitor(monitor))(readAction))
  }

  override def newProducer(configName: String, monitor: Monitor): RabbitMQProducer = {
    new RabbitMQProducer(scalaFactory.newProducer(configName, ScalaMonitor(monitor)))
  }

  override def newConsumer(config: ConsumerConfig, monitor: Monitor, executor: Executor)(
      readAction: function.Function[Delivery, CompletableFuture[DeliveryResult]]): RabbitMQConsumer = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    new RabbitMQConsumer(scalaFactory.newConsumer(config, ScalaMonitor(monitor))(readAction))
  }

  override def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer = {
    new RabbitMQProducer(scalaFactory.newProducer(config, ScalaMonitor(monitor)))
  }

  override def close(): Unit = scalaFactory.close()
}
