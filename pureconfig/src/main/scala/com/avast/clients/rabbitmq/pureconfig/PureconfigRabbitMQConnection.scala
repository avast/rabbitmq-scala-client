package com.avast.clients.rabbitmq.pureconfig

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.{
  ChannelListener,
  ConnectionListener,
  ConsumerListener,
  DeliveryConverter,
  DeliveryReadAction,
  ProductConverter,
  RabbitMQConnection,
  ServerChannel
}
import com.avast.metrics.scalaapi.Monitor
import com.typesafe.config.Config

import scala.language.higherKinds

trait PureconfigRabbitMQConnection[F[_]] {

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

class DefaultPureconfigRabbitMQConnection[F[_]](config: Config, wrapped: RabbitMQConnection[F])(implicit F: ConcurrentEffect[F],
                                                                                                timer: Timer[F],
                                                                                                cs: ContextShift[F])
    extends PureconfigRabbitMQConnection[F] {

  override def newChannel(): Resource[F, ServerChannel] = wrapped.newChannel()

  override def withChannel[A](f: ServerChannel => F[A]): F[A] = wrapped.withChannel(f)

  override val connectionListener: ConnectionListener = wrapped.connectionListener

  override val channelListener: ChannelListener = wrapped.channelListener

  override val consumerListener: ConsumerListener = wrapped.consumerListener

  override def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = ???

  override def newProducer[A: ProductConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]] = ???

  override def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]] = ???

  override def declareExchange(configName: String): F[Unit] = ???

  override def declareQueue(configName: String): F[Unit] = ???

  override def bindQueue(configName: String): F[Unit] = ???

  override def bindExchange(configName: String): F[Unit] = ???
}
