package com.avast.clients.rabbitmq.pureconfig

import _root_.pureconfig.ConfigReader.Result
import _root_.pureconfig._
import _root_.pureconfig.generic.ProductHint
import _root_.pureconfig.generic.semiauto._
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.clients.rabbitmq.api.DeliveryResult._
import com.avast.clients.rabbitmq.{
  AutoBindExchangeConfig,
  AutoBindQueueConfig,
  AutoDeclareExchangeConfig,
  AutoDeclareQueueConfig,
  BindArgumentsConfig,
  BindExchangeConfig,
  BindQueueConfig,
  ConsumerConfig,
  CredentialsConfig,
  DeclareArgumentsConfig,
  DeclareExchangeConfig,
  DeclareQueueConfig,
  ExchangeType,
  NetworkRecoveryConfig,
  ProducerConfig,
  ProducerPropertiesConfig,
  PullConsumerConfig,
  RabbitMQConnectionConfig,
  RecoveryDelayHandlers
}
import com.rabbitmq.client.RecoveryDelayHandler
import org.slf4j.event.Level

// scalastyle:off
object implicits extends PureconfigImplicits( /* use default */ ) {
  val CamelCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.CamelCase)
  val KebabCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.KebabCase)
  val SnakeCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.SnakeCase)
  val PascalCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.PascalCase)
}
// scalastyle:on

class PureconfigImplicits(implicit namingConvention: NamingConvention = CamelCase) {

  private implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(namingConvention, namingConvention))

  // connection, producer, consumers:
  implicit val connectionConfigReader: ConfigReader[RabbitMQConnectionConfig] = deriveReader
  implicit val consumerConfigReader: ConfigReader[ConsumerConfig] = deriveReader
  implicit val pullConsumerConfigReader: ConfigReader[PullConsumerConfig] = deriveReader
  implicit val producerConfigReader: ConfigReader[ProducerConfig] = deriveReader

  // additional declarations:
  implicit val declareExchangeConfigReader: ConfigReader[DeclareExchangeConfig] = deriveReader
  implicit val declareQueueConfigReader: ConfigReader[DeclareQueueConfig] = deriveReader
  implicit val bindQueueConfigReader: ConfigReader[BindQueueConfig] = deriveReader
  implicit val bindExchangeConfigReader: ConfigReader[BindExchangeConfig] = deriveReader

  // "internal":
  implicit val autoDeclareQueueConfigReader: ConfigReader[AutoDeclareQueueConfig] = deriveReader
  implicit val autoDeclareExchangeConfigReader: ConfigReader[AutoDeclareExchangeConfig] = deriveReader
  implicit val networkRecoveryConfigReader: ConfigReader[NetworkRecoveryConfig] = deriveReader
  implicit val credentialsConfigReader: ConfigReader[CredentialsConfig] = deriveReader
  implicit val autoBindQueueConfigReader: ConfigReader[AutoBindQueueConfig] = deriveReader
  implicit val autoBindExchangeConfigReader: ConfigReader[AutoBindExchangeConfig] = deriveReader
  implicit val producerPropertiesConfigReader: ConfigReader[ProducerPropertiesConfig] = deriveReader
  implicit val declareArgumentsConfigReader: ConfigReader[DeclareArgumentsConfig] = deriveReader
  implicit val bindArgumentsConfigReader: ConfigReader[BindArgumentsConfig] = deriveReader

  implicit val logLevelReader: ConfigReader[Level] = ConfigReader.stringConfigReader.map(Level.valueOf)
  implicit val recoveryDelayHandlerReader: ConfigReader[RecoveryDelayHandler] = RecoveryDelayHandlerReader
  implicit val exchangeTypeReader: ConfigReader[ExchangeType] = ConfigReader.fromNonEmptyStringOpt(ExchangeType.apply)

  implicit val deliveryResultReader: ConfigReader[DeliveryResult] = ConfigReader.stringConfigReader.map {
    _.toLowerCase match {
      case "ack" => Ack
      case "reject" => Reject
      case "retry" => Retry
      case "republish" => Republish()
    }
  }

  implicit val mapStringAnyReader: ConfigReader[Map[String, Any]] = ConfigReader.fromCursor { cur =>
    import scala.collection.JavaConverters._

    cur.asObjectCursor.map(_.value.asScala.toMap.mapValues(_.unwrapped()))
  }

  private object RecoveryDelayHandlerReader extends ConfigReader[RecoveryDelayHandler] {
    implicit val linearReader: ConfigReader[RecoveryDelayHandlers.Linear] = deriveReader
    implicit val exponentialReader: ConfigReader[RecoveryDelayHandlers.Exponential] = deriveReader

    override def from(cur: ConfigCursor): Result[RecoveryDelayHandler] = {
      cur.fluent.at("type").asString.map(_.toLowerCase).flatMap {
        case "linear" => ConfigReader[RecoveryDelayHandlers.Linear].from(cur)
        case "exponential" => ConfigReader[RecoveryDelayHandlers.Exponential].from(cur)
      }
    }
  }
}
