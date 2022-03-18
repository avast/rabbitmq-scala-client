package com.avast.clients.rabbitmq

import cats.effect.{ContextShift, Sync}
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.rabbitmq.client.RecoveryDelayHandler
import org.slf4j.event.Level

import scala.collection.immutable
import scala.concurrent.duration._

// Beside actual fields if this case class, `producers` + `consumers` + `declarations elems are requires in the configuration, even if empty
final case class RabbitMQConnectionConfig(name: String,
                                          hosts: immutable.Seq[String],
                                          virtualHost: String,
                                          addressResolverType: AddressResolverType = AddressResolverType.Default,
                                          connectionTimeout: FiniteDuration = 5.seconds,
                                          heartBeatInterval: FiniteDuration = 30.seconds,
                                          topologyRecovery: Boolean = true,
                                          networkRecovery: NetworkRecoveryConfig = NetworkRecoveryConfig(),
                                          channelMax: Int = 2047,
                                          credentials: CredentialsConfig,
                                          republishStrategy: RepublishStrategyConfig = RepublishStrategyConfig.DefaultExchange)

final case class NetworkRecoveryConfig(enabled: Boolean = true, handler: RecoveryDelayHandler = RecoveryDelayHandlers.Linear())

final case class CredentialsConfig(enabled: Boolean = true, username: String, password: String)

final case class ConsumerConfig(name: String,
                                queueName: String,
                                bindings: immutable.Seq[AutoBindQueueConfig],
                                processTimeout: FiniteDuration = 10.seconds,
                                failureAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutLogLevel: Level = Level.WARN,
                                prefetchCount: Int = 100,
                                declare: Option[AutoDeclareQueueConfig] = None,
                                consumerTag: String = "Default",
                                poisonedMessageHandling: Option[PoisonedMessageHandlingConfig] = None)

final case class StreamingConsumerConfig(name: String,
                                         queueName: String,
                                         bindings: immutable.Seq[AutoBindQueueConfig],
                                         processTimeout: FiniteDuration = 10.seconds,
                                         timeoutAction: DeliveryResult = DeliveryResult.Republish(),
                                         timeoutLogLevel: Level = Level.WARN,
                                         prefetchCount: Int = 100,
                                         queueBufferSize: Int = 100,
                                         declare: Option[AutoDeclareQueueConfig] = None,
                                         consumerTag: String = "Default",
                                         poisonedMessageHandling: Option[PoisonedMessageHandlingConfig] = None)

final case class PullConsumerConfig(name: String,
                                    queueName: String,
                                    bindings: immutable.Seq[AutoBindQueueConfig],
                                    declare: Option[AutoDeclareQueueConfig] = None,
                                    poisonedMessageHandling: Option[PoisonedMessageHandlingConfig] = None)

final case class AutoDeclareQueueConfig(enabled: Boolean = false,
                                        durable: Boolean = true,
                                        exclusive: Boolean = false,
                                        autoDelete: Boolean = false,
                                        arguments: DeclareArgumentsConfig = DeclareArgumentsConfig())

final case class DeclareArgumentsConfig(value: Map[String, Any] = Map.empty)

final case class BindArgumentsConfig(value: Map[String, Any] = Map.empty)

final case class AutoBindQueueConfig(exchange: AutoBindExchangeConfig,
                                     routingKeys: immutable.Seq[String],
                                     bindArguments: BindArgumentsConfig = BindArgumentsConfig())

final case class AutoBindExchangeConfig(name: String, declare: Option[AutoDeclareExchangeConfig] = None)

final case class ProducerConfig(name: String,
                                exchange: String,
                                declare: Option[AutoDeclareExchangeConfig] = None,
                                reportUnroutable: Boolean = true,
                                properties: ProducerPropertiesConfig = ProducerPropertiesConfig())

final case class ProducerPropertiesConfig(deliveryMode: Int = 2,
                                          contentType: Option[String] = None,
                                          contentEncoding: Option[String] = None,
                                          priority: Option[Int] = None)

final case class AutoDeclareExchangeConfig(enabled: Boolean,
                                           `type`: ExchangeType,
                                           durable: Boolean = true,
                                           autoDelete: Boolean = false,
                                           arguments: DeclareArgumentsConfig = DeclareArgumentsConfig())

final case class DeclareExchangeConfig(name: String,
                                       `type`: ExchangeType,
                                       durable: Boolean = true,
                                       autoDelete: Boolean = false,
                                       arguments: DeclareArgumentsConfig = DeclareArgumentsConfig())

final case class DeclareQueueConfig(name: String,
                                    durable: Boolean = true,
                                    exclusive: Boolean = false,
                                    autoDelete: Boolean = false,
                                    arguments: DeclareArgumentsConfig = DeclareArgumentsConfig())

final case class BindQueueConfig(queueName: String,
                                 exchangeName: String,
                                 routingKeys: immutable.Seq[String],
                                 arguments: BindArgumentsConfig = BindArgumentsConfig())

final case class BindExchangeConfig(sourceExchangeName: String,
                                    destExchangeName: String,
                                    routingKeys: immutable.Seq[String],
                                    arguments: BindArgumentsConfig = BindArgumentsConfig())

sealed trait PoisonedMessageHandlingConfig

final case class DeadQueueProducerConfig(name: String,
                                         exchange: String,
                                         routingKey: String,
                                         declare: Option[AutoDeclareExchangeConfig] = None,
                                         reportUnroutable: Boolean = true,
                                         properties: ProducerPropertiesConfig = ProducerPropertiesConfig())

case object NoOpPoisonedMessageHandling extends PoisonedMessageHandlingConfig
final case class LoggingPoisonedMessageHandling(maxAttempts: Int) extends PoisonedMessageHandlingConfig
final case class DeadQueuePoisonedMessageHandling(maxAttempts: Int, deadQueueProducer: DeadQueueProducerConfig)
    extends PoisonedMessageHandlingConfig

sealed trait AddressResolverType
object AddressResolverType {
  case object Default extends AddressResolverType
  case object List extends AddressResolverType
  case object DnsRecord extends AddressResolverType
  case object DnsSrvRecord extends AddressResolverType
}

sealed trait ExchangeType {
  val value: String
}
object ExchangeType {
  def apply(name: String): Option[ExchangeType] = name.toLowerCase match {
    case Direct.value => Some(Direct)
    case Fanout.value => Some(Fanout)
    case Topic.value => Some(Topic)
    case n if n.startsWith("x-") => Some(Custom(n))
    case _ => None
  }

  case object Direct extends ExchangeType { val value: String = "direct" }
  case object Fanout extends ExchangeType { val value: String = "fanout" }
  case object Topic extends ExchangeType { val value: String = "topic" }

  case class Custom(value: String) extends ExchangeType
}

trait RepublishStrategyConfig {
  def toRepublishStrategy[F[_]: Sync: ContextShift]: RepublishStrategy[F]
}

object RepublishStrategyConfig {
  case class CustomExchange(exchangeName: String, exchangeDeclare: Boolean = true, exchangeAutoBind: Boolean = true)
      extends RepublishStrategyConfig {
    override def toRepublishStrategy[F[_]: Sync: ContextShift]: RepublishStrategy[F] = RepublishStrategy.CustomExchange(exchangeName)
  }

  case object DefaultExchange extends RepublishStrategyConfig {
    override def toRepublishStrategy[F[_]: Sync: ContextShift]: RepublishStrategy[F] = RepublishStrategy.DefaultExchange[F]()
  }
}
