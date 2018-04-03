package com.avast.clients.rabbitmq

import java.nio.file.Path
import java.time.Duration

import com.avast.clients.rabbitmq.api.DeliveryResult
import com.typesafe.config.Config

import scala.collection.immutable

case class RabbitMQConnectionConfig(hosts: Array[String],
                                    name: String,
                                    virtualHost: String,
                                    connectionTimeout: Duration,
                                    heartBeatInterval: Duration,
                                    topologyRecovery: Boolean,
                                    networkRecovery: NetworkRecovery,
                                    credentials: Credentials,
                                    ssl: Ssl)

case class NetworkRecovery(enabled: Boolean, period: Duration)

case class Credentials(enabled: Boolean, username: String, password: String)

case class Ssl(enabled: Boolean, trustStore: TrustStore)

case class TrustStore(path: Path, password: String)

private[rabbitmq] case class RabbitMQConnectionInfo(hosts: immutable.Seq[String], virtualHost: String)

case class ConsumerConfig(queueName: String,
                          processTimeout: Duration,
                          failureAction: DeliveryResult,
                          timeoutAction: DeliveryResult,
                          prefetchCount: Int,
                          useKluzo: Boolean,
                          declare: AutoDeclareQueue,
                          bindings: immutable.Seq[AutoBindQueue],
                          consumerTag: String,
                          name: String)

case class AutoDeclareQueue(enabled: Boolean, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, arguments: DeclareArguments)

case class DeclareArguments(value: Map[String, Any])

case class BindArguments(value: Map[String, Any])

case class AutoBindQueue(exchange: AutoBindExchange, routingKeys: immutable.Seq[String], bindArguments: BindArguments)

case class AutoBindExchange(name: String, declare: Config)

case class ProducerConfig(exchange: String, declare: Config, useKluzo: Boolean, reportUnroutable: Boolean, name: String)

case class AutoDeclareExchange(enabled: Boolean, `type`: String, durable: Boolean, autoDelete: Boolean, arguments: DeclareArguments)

case class DeclareExchange(name: String, `type`: String, durable: Boolean, autoDelete: Boolean, arguments: DeclareArguments)

case class DeclareQueue(name: String, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, arguments: DeclareArguments)

case class BindQueue(queueName: String, exchangeName: String, routingKeys: immutable.Seq[String], bindArguments: BindArguments)

case class BindExchange(sourceExchangeName: String, destExchangeName: String, routingKeys: immutable.Seq[String], arguments: BindArguments)
