package com.avast.clients.rabbitmq.pureconfig

import _root_.pureconfig._
import _root_.pureconfig.error.ConfigReaderException
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
  ServerChannel,
  StreamingConsumerConfig
}
import com.avast.metrics.scalaapi.Monitor

import scala.reflect.ClassTag

trait ConfigRabbitMQConnection[F[_]] {

  def newChannel(): Resource[F, ServerChannel]

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed F.
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

  /** Creates new instance of streaming consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    */
  def newStreamingConsumer[A: DeliveryConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQStreamingConsumer[F, A]]

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

  def connectionListener: ConnectionListener[F]
  def channelListener: ChannelListener[F]
  def consumerListener: ConsumerListener[F]
}

class DefaultConfigRabbitMQConnection[F[_]](config: ConfigCursor, wrapped: RabbitMQConnection[F])(
    implicit F: ConcurrentEffect[F],
    consumerConfigReader: ConfigReader[ConsumerConfig],
    producerConfigReader: ConfigReader[ProducerConfig],
    pullConsumerConfigReader: ConfigReader[PullConsumerConfig],
    streamingConsumerConfigReader: ConfigReader[StreamingConsumerConfig],
    declareExchangeConfigReader: ConfigReader[DeclareExchangeConfig],
    declareQueueConfigReader: ConfigReader[DeclareQueueConfig],
    bindQueueConfigReader: ConfigReader[BindQueueConfig],
    bindExchangeConfigReader: ConfigReader[BindExchangeConfig]
) extends ConfigRabbitMQConnection[F] {
  import cats.syntax.flatMap._

  override def newChannel(): Resource[F, ServerChannel] = wrapped.newChannel()

  override def withChannel[A](f: ServerChannel => F[A]): F[A] = wrapped.withChannel(f)

  override val connectionListener: ConnectionListener[F] = wrapped.connectionListener

  override val channelListener: ChannelListener[F] = wrapped.channelListener

  override val consumerListener: ConsumerListener[F] = wrapped.consumerListener

  override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = {
    Resource.eval(loadConfig[ConsumerConfig](ConsumersRootName, configName)) >>= (wrapped.newConsumer(_, monitor)(readAction))
  }

  override def newProducer[A: ProductConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]] = {
    Resource.eval(loadConfig[ProducerConfig](ProducersRootName, configName)) >>= (wrapped.newProducer(_, monitor))
  }

  override def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]] = {
    Resource.eval(loadConfig[PullConsumerConfig](ConsumersRootName, configName)) >>= (wrapped.newPullConsumer(_, monitor))
  }

  override def newStreamingConsumer[A: DeliveryConverter](configName: String,
                                                          monitor: Monitor): Resource[F, RabbitMQStreamingConsumer[F, A]] = {
    Resource.eval(loadConfig[StreamingConsumerConfig](ConsumersRootName, configName)) >>= (wrapped.newStreamingConsumer(_, monitor))
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
      val segments: Seq[PathSegment.Key] = (section +: name.split('.').toSeq).map(PathSegment.Key)

      config.fluent
        .at(segments: _*)
        .cursor
        .flatMap(reader.from)
        .fold(errs => throw ConfigReaderException(errs), identity)
    }
  }
}
