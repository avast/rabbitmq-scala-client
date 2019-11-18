package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.api.DeliveryResult
import com.rabbitmq.client.RecoveryDelayHandler
import org.slf4j.event.Level

import scala.collection.immutable
import scala.concurrent.duration._

final case class RabbitMQConnectionConfig(hosts: immutable.Seq[String],
                                          name: String,
                                          virtualHost: String,
                                          connectionTimeout: FiniteDuration = 5.seconds,
                                          heartBeatInterval: FiniteDuration = 30.seconds,
                                          topologyRecovery: Boolean = true,
                                          networkRecovery: NetworkRecoveryConfig = NetworkRecoveryConfig(),
                                          credentials: CredentialsConfig)

final case class NetworkRecoveryConfig(enabled: Boolean = true, handler: RecoveryDelayHandler = RecoveryDelayHandlers.Linear())

final case class CredentialsConfig(enabled: Boolean = true, username: String, password: String)

final case class ConsumerConfig(queueName: String,
                                processTimeout: FiniteDuration = 10.seconds,
                                failureAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutLogLevel: Level = Level.WARN,
                                prefetchCount: Int = 100,
                                declare: Option[AutoDeclareQueueConfig] = None,
                                bindings: immutable.Seq[AutoBindQueueConfig],
                                consumerTag: String = "Default",
                                name: String)

final case class PullConsumerConfig(queueName: String,
                                    failureAction: DeliveryResult = DeliveryResult.Republish(),
                                    declare: Option[AutoDeclareQueueConfig] = None,
                                    bindings: immutable.Seq[AutoBindQueueConfig],
                                    name: String)

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

final case class ProducerConfig(exchange: String,
                                declare: Option[AutoDeclareExchangeConfig] = None,
                                reportUnroutable: Boolean = true,
                                name: String,
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

sealed trait ExchangeType {
  val value: String
}
object ExchangeType {
  def apply(name: String): Option[ExchangeType] = name.toLowerCase match {
    case Direct.value => Some(Direct)
    case Fanout.value => Some(Fanout)
    case Topic.value => Some(Topic)
    case _ => None
  }

  case object Direct extends ExchangeType { val value: String = "direct" }
  case object Fanout extends ExchangeType { val value: String = "fanout" }
  case object Topic extends ExchangeType { val value: String = "topic" }
}
