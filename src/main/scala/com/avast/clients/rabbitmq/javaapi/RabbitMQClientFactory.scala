//package com.avast.clients.rabbitmq.javaapi
//
//import java.util.concurrent.{CompletableFuture, Executor, ScheduledExecutorService}
//import java.util.function.{Function => JavaFunction}
//import javax.annotation.{Nonnull, Nullable}
//
//import com.avast.clients.rabbitmq.api.{RabbitMQConsumer, RabbitMQProducer}
//import com.avast.clients.rabbitmq.javaapi.{DeliveryResult => JavaResult, RabbitMQChannelFactory => JavaChannelFactory}
//import com.avast.clients.rabbitmq.{
//  ConsumerConfig,
//  DefaultRabbitMQProducer,
//  ProducerConfig,
//  Delivery => ScalaDelivery,
//  DeliveryResult => ScalaResult,
//  RabbitMQClientFactory => ScalaFactory
//}
//import com.avast.metrics.api.Monitor
//import com.avast.metrics.scalaapi.{Monitor => ScalaMonitor}
//import com.avast.utils2.JavaConverters._
//import com.avast.utils2.errorhandling.FutureTimeouter
//import com.typesafe.config.Config
//
//import scala.concurrent.{ExecutionContext, Future}
//import scala.language.implicitConversions
//
//object RabbitMQClientFactory {
//
//  /** Creates new instance of producer, using the passed TypeSafe configuration.
//    *
//    * @param providedConfig The configuration.
//    * @param channelFactory See [[RabbitMQChannelFactory]].
//    * @param monitor        Monitor for metrics.
//    */
//  def createProducerfromConfig(@Nonnull providedConfig: Config,
//                               @Nonnull channelFactory: JavaChannelFactory,
//                               @Nonnull monitor: Monitor): RabbitMQProducer = {
//    ScalaFactory.Producer.fromConfig(providedConfig, channelFactory, ScalaMonitor(monitor))
//  }
//
//  /** Creates new instance of producer, using the passed configuration.
//    *
//    * @param producerConfig The configuration.
//    * @param channelFactory See [[RabbitMQChannelFactory]].
//    * @param monitor        Monitor for metrics.
//    */
//  def createProducer(@Nonnull producerConfig: ProducerConfig,
//                     @Nonnull channelFactory: JavaChannelFactory,
//                     @Nonnull monitor: Monitor): DefaultRabbitMQProducer = {
//    ScalaFactory.Producer.create(producerConfig, channelFactory, ScalaMonitor(monitor))
//  }
//
//  /** Creates new instance of consumer, using the passed TypeSafe configuration.
//    *
//    * @param providedConfig           The configuration.
//    * @param channelFactory           See [[RabbitMQChannelFactory]].
//    * @param monitor                  Monitor for metrics.
//    * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks (after specified timeout).
//    * @param callbackExecutor         [[Executor]] used for callbacks.
//    * @param readAction               Action executed for each delivered message. You should never return a failed future.
//    */
//  def createConsumerfromConfig(@Nonnull providedConfig: Config,
//                               @Nonnull channelFactory: JavaChannelFactory,
//                               @Nonnull monitor: Monitor,
//                               @Nullable scheduledExecutorService: ScheduledExecutorService,
//                               @Nonnull callbackExecutor: Executor,
//                               @Nonnull readAction: JavaFunction[Delivery, CompletableFuture[JavaResult]]): RabbitMQConsumer = {
//    implicit val ec = ExecutionContext.fromExecutor(callbackExecutor)
//    val scalaReadAction: ScalaDelivery => Future[ScalaResult] = d => readAction(d).asScala.map(ScalaResult.apply)
//
//    ScalaFactory.Consumer.fromConfig(
//      providedConfig,
//      channelFactory,
//      ScalaMonitor(monitor),
//      Option(scheduledExecutorService).getOrElse(FutureTimeouter.Implicits.DefaultScheduledExecutor))(scalaReadAction)(ec)
//
//  }
//
//  /** Creates new instance of consumer, using the passed configuration.
//    *
//    * @param consumerConfig           The configuration.
//    * @param channelFactory           See [[RabbitMQChannelFactory]].
//    * @param monitor                  Monitor for metrics.
//    * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks (after specified timeout).
//    * @param callbackExecutor         [[Executor]] used for callbacks.
//    * @param readAction               Action executed for each delivered message. You should never return a failed future.
//    */
//  def createConsumer(@Nonnull consumerConfig: ConsumerConfig,
//                     @Nonnull channelFactory: JavaChannelFactory,
//                     @Nonnull monitor: Monitor,
//                     @Nullable scheduledExecutorService: ScheduledExecutorService,
//                     @Nonnull callbackExecutor: Executor,
//                     @Nonnull readAction: JavaFunction[Delivery, CompletableFuture[JavaResult]]): RabbitMQConsumer = {
//    implicit val ec = ExecutionContext.fromExecutor(callbackExecutor)
//    val scalaReadAction: ScalaDelivery => Future[ScalaResult] = d => readAction(d).asScala.map(ScalaResult.apply)
//
//    ScalaFactory.Consumer.create(
//      consumerConfig,
//      channelFactory,
//      ScalaMonitor(monitor),
//      Option(scheduledExecutorService).getOrElse(FutureTimeouter.Implicits.DefaultScheduledExecutor))(scalaReadAction)
//  }
//
//  private implicit def scalaDeliveryToJava(d: ScalaDelivery): Delivery = {
//    new Delivery(d)
//  }
//
//}
