package com.avast.clients

import java.nio.file.Path
import java.time.Duration

import com.typesafe.config.Config

import scala.collection.immutable

package object rabbitmq {
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

  case class RabbitMqFactoryInfo(hosts: immutable.Seq[String], virtualHost: String)

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

  case class AutoDeclareQueue(enabled: Boolean, durable: Boolean, exclusive: Boolean, autoDelete: Boolean)

  case class AutoBindQueue(exchange: BindExchange, routingKeys: immutable.Seq[String])

  case class BindExchange(name: String, declare: Config)

  case class ProducerConfig(exchange: String, declare: Config, useKluzo: Boolean, reportUnroutable: Boolean, name: String)

  case class AutoDeclareExchange(enabled: Boolean, `type`: String, durable: Boolean, autoDelete: Boolean)
}
