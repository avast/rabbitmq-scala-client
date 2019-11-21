package com.avast.clients.rabbitmq.pureconfig

import _root_.pureconfig.error.ConfigReaderException
import _root_.pureconfig.{ConfigCursor, ConfigReader}
import cats.effect.{ConcurrentEffect, Resource}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.{
  BindExchangeConfig,
  BindQueueConfig,
  ChannelListener,
  ConnectionListener,
  ConsumerConfig,
  ConsumerListener,
  DeclareExchangeConfig,
  DeclareQueueConfig,
  DeliveryConverter,
  DeliveryReadAction,
  ProducerConfig,
  ProductConverter,
  PullConsumerConfig,
  RabbitMQConnection,
  ServerChannel
}
import com.avast.metrics.scalaapi.Monitor

import scala.language.higherKinds
import scala.reflect.ClassTag

trait ConfigRabbitMQConnection[F[_]] {

  def newChannel(): Resource[F, ServerChannel]

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    */
  def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]]

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param monitor    Monitor for metrics.
    */
  def newProducer[A: ProductConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]]

  /** Creates new instance of pull consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    */
  def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]]

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareExchange(configName: String): F[Unit]

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareQueue(configName: String): F[Unit]

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindQueue(configName: String): F[Unit]

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindExchange(configName: String): F[Unit]

  /** Executes a specified action with newly created [[ServerChannel]] which is then closed.
    *
    * @see #newChannel()
    * @return Result of performed action.
    */
  def withChannel[A](f: ServerChannel => F[A]): F[A]

  def connectionListener: ConnectionListener
  def channelListener: ChannelListener
  def consumerListener: ConsumerListener
}

class DefaultConfigRabbitMQConnection[F[_]](config: ConfigCursor, wrapped: RabbitMQConnection[F])(
    implicit F: ConcurrentEffect[F],
    consumerConfigReader: ConfigReader[ConsumerConfig],
    producerConfigReader: ConfigReader[ProducerConfig],
    pullConsumerConfigReader: ConfigReader[PullConsumerConfig],
    declareExchangeConfigReader: ConfigReader[DeclareExchangeConfig],
    declareQueueConfigReader: ConfigReader[DeclareQueueConfig],
    bindQueueConfigReader: ConfigReader[BindQueueConfig],
    bindExchangeConfigReader: ConfigReader[BindExchangeConfig]
) extends ConfigRabbitMQConnection[F] {
  import cats.syntax.flatMap._

  override def newChannel(): Resource[F, ServerChannel] = wrapped.newChannel()

  override def withChannel[A](f: ServerChannel => F[A]): F[A] = wrapped.withChannel(f)

  override val connectionListener: ConnectionListener = wrapped.connectionListener

  override val channelListener: ChannelListener = wrapped.channelListener

  override val consumerListener: ConsumerListener = wrapped.consumerListener

  override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = {
    Resource.liftF(loadConfig[ConsumerConfig](ConsumersRootName, configName)) >>= (wrapped.newConsumer(_, monitor)(readAction))
  }

  override def newProducer[A: ProductConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]] = {
    Resource.liftF(loadConfig[ProducerConfig](ProducersRootName, configName)) >>= (wrapped.newProducer(_, monitor))
  }

  override def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]] = {
    Resource.liftF(loadConfig[PullConsumerConfig](ConsumersRootName, configName)) >>= (wrapped.newPullConsumer(_, monitor))
  }

  override def declareExchange(configName: String): F[Unit] = {
    loadConfig[DeclareExchangeConfig](DeclarationsRootName, configName) >>= wrapped.declareExchange
  }

  override def declareQueue(configName: String): F[Unit] = {
    loadConfig[DeclareQueueConfig](DeclarationsRootName, configName) >>= wrapped.declareQueue
  }

  override def bindQueue(configName: String): F[Unit] = {
    loadConfig[BindQueueConfig](DeclarationsRootName, configName) >>= wrapped.bindQueue
  }

  override def bindExchange(configName: String): F[Unit] = {
    loadConfig[BindExchangeConfig](DeclarationsRootName, configName) >>= wrapped.bindExchange
  }

  private def loadConfig[C](section: String, name: String)(implicit ct: ClassTag[C], reader: ConfigReader[C]): F[C] = {
    F.delay {
      config.fluent.at(section).at(name).cursor.flatMap(reader.from).fold(errs => throw ConfigReaderException(errs), identity)
    }
  }
}
