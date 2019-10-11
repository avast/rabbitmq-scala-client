package com.avast.clients.rabbitmq

import java.nio.file.Path

import com.avast.clients.rabbitmq.api.DeliveryResult
import com.rabbitmq.client.RecoveryDelayHandler
import org.slf4j.event.Level

import scala.collection.immutable
import scala.concurrent.duration._

final case class RabbitMQConnectionConfig(hosts: immutable.Seq[String],
                                          name: String,
                                          virtualHost: String = "",
                                          connectionTimeout: FiniteDuration = 5.seconds,
                                          heartBeatInterval: FiniteDuration = 30.seconds,
                                          topologyRecovery: Boolean = true,
                                          networkRecovery: NetworkRecovery = NetworkRecovery(),
                                          credentials: Credentials,
                                          ssl: Ssl = Ssl())

final case class NetworkRecovery(enabled: Boolean = true, handler: RecoveryDelayHandler = RecoveryDelayHandlers.Linear())

final case class Credentials(enabled: Boolean = true, username: String, password: String)

final case class Ssl(enabled: Boolean = true, trustStore: Option[TrustStore] = None)

final case class TrustStore(path: Path, password: String)

final case class ConsumerConfig(queueName: String,
                                processTimeout: FiniteDuration = 10.seconds,
                                failureAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutAction: DeliveryResult = DeliveryResult.Republish(),
                                timeoutLogLevel: Level = Level.WARN,
                                prefetchCount: Int = 100,
                                declare: Option[AutoDeclareQueue] = None,
                                bindings: immutable.Seq[AutoBindQueue],
                                consumerTag: String,
                                name: String)

final case class PullConsumerConfig(queueName: String,
                                    failureAction: DeliveryResult = DeliveryResult.Republish(),
                                    declare: Option[AutoDeclareQueue] = None,
                                    bindings: immutable.Seq[AutoBindQueue],
                                    name: String)

final case class AutoDeclareQueue(enabled: Boolean = true,
                                  durable: Boolean,
                                  exclusive: Boolean,
                                  autoDelete: Boolean,
                                  arguments: DeclareArguments = DeclareArguments())

final case class DeclareArguments(value: Map[String, Any] = Map.empty)

final case class BindArguments(value: Map[String, Any] = Map.empty)

final case class AutoBindQueue(exchange: AutoBindExchange,
                               routingKeys: immutable.Seq[String],
                               bindArguments: BindArguments = BindArguments())

final case class AutoBindExchange(name: String, declare: Option[AutoDeclareExchange] = None)

final case class ProducerConfig(exchange: String,
                                declare: Option[AutoDeclareExchange] = None,
                                reportUnroutable: Boolean = true,
                                name: String,
                                properties: ProducerProperties = ProducerProperties())

final case class ProducerProperties(deliveryMode: Int = 2,
                                    contentType: Option[String] = None,
                                    contentEncoding: Option[String] = None,
                                    priority: Option[Int] = None)

final case class AutoDeclareExchange(enabled: Boolean,
                                     `type`: ExchangeType,
                                     durable: Boolean = true,
                                     autoDelete: Boolean = false,
                                     arguments: DeclareArguments = DeclareArguments())

final case class DeclareExchange(name: String,
                                 `type`: ExchangeType,
                                 durable: Boolean = true,
                                 autoDelete: Boolean = false,
                                 arguments: DeclareArguments = DeclareArguments())

final case class DeclareQueue(name: String,
                              durable: Boolean = true,
                              exclusive: Boolean = false,
                              autoDelete: Boolean = false,
                              arguments: DeclareArguments = DeclareArguments())

final case class BindQueue(queueName: String,
                           exchangeName: String,
                           routingKeys: immutable.Seq[String],
                           arguments: BindArguments = BindArguments())

final case class BindExchange(sourceExchangeName: String,
                              destExchangeName: String,
                              routingKeys: immutable.Seq[String],
                              arguments: BindArguments = BindArguments())

sealed trait ExchangeType {
  val value: String
}
object ExchangeType {
  case object Direct extends ExchangeType { val value: String = "direct" }
  case object Fanout extends ExchangeType { val value: String = "fanout" }
  case object Topic extends ExchangeType { val value: String = "topic" }
}
