package com.avast.clients.rabbitmq.pureconfig

import _root_.pureconfig.ConfigReader.Result
import _root_.pureconfig._
import _root_.pureconfig.generic.ProductHint
import _root_.pureconfig.generic.semiauto._
import cats.data.NonEmptyList
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.clients.rabbitmq.api.DeliveryResult._
import com.avast.clients.rabbitmq.{pureconfig => _, _}
import com.rabbitmq.client.RecoveryDelayHandler
import org.slf4j.event.Level
import pureconfig.error._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

// scalastyle:off
object implicits extends PureconfigImplicits( /* use defaults */ ) {
  val CamelCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.CamelCase)
  val KebabCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.KebabCase)
  val SnakeCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.SnakeCase)
  val PascalCase: PureconfigImplicits = new PureconfigImplicits()(namingConvention = _root_.pureconfig.PascalCase)

  object nonStrict extends PureconfigImplicits()(checkPolicy = ConfigurationCheckPolicy.NonStrict) {

    val CamelCase: PureconfigImplicits = new PureconfigImplicits()(
      namingConvention = _root_.pureconfig.CamelCase,
      checkPolicy = ConfigurationCheckPolicy.NonStrict
    )

    val KebabCase: PureconfigImplicits = new PureconfigImplicits()(
      namingConvention = _root_.pureconfig.KebabCase,
      checkPolicy = ConfigurationCheckPolicy.NonStrict
    )

    val SnakeCase: PureconfigImplicits = new PureconfigImplicits()(
      namingConvention = _root_.pureconfig.SnakeCase,
      checkPolicy = ConfigurationCheckPolicy.NonStrict
    )

    val PascalCase: PureconfigImplicits = new PureconfigImplicits()(
      namingConvention = _root_.pureconfig.PascalCase,
      checkPolicy = ConfigurationCheckPolicy.NonStrict
    )
  }
}
// scalastyle:on

class PureconfigImplicits(implicit namingConvention: NamingConvention = CamelCase,
                          checkPolicy: ConfigurationCheckPolicy = ConfigurationCheckPolicy.Strict) {
  import PureconfigImplicits._

  private val allowUnknownKeys: Boolean = checkPolicy match {
    case ConfigurationCheckPolicy.Strict => false
    case ConfigurationCheckPolicy.NonStrict => true
  }

  private implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, namingConvention), allowUnknownKeys = allowUnknownKeys)

  // connection, producer, consumers:
  implicit val connectionConfigReader: ConfigReader[RabbitMQConnectionConfig] = {
    if (!allowUnknownKeys) StrictConnectionConfigReader else deriveReader[RabbitMQConnectionConfig]
  }
  implicit val consumerConfigReader: ConfigReader[ConsumerConfig] = deriveReader
  implicit val pullConsumerConfigReader: ConfigReader[PullConsumerConfig] = deriveReader
  implicit val streamingConsumerConfigReader: ConfigReader[StreamingConsumerConfig] = deriveReader
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
  implicit val addressResolverTypeReader: ConfigReader[AddressResolverType] = ConfigReader.fromNonEmptyStringTry {
    case "Default" => Success(AddressResolverType.Default)
    case "ListAddress" => Success(AddressResolverType.List)
    case "DnsRecordIpAddress" => Success(AddressResolverType.DnsRecord)
    case "DnsSrvRecordAddress" => Success(AddressResolverType.DnsSrvRecord)
    case unknownName => Failure(new IllegalArgumentException(s"Unknown addressResolverType: $unknownName"))
  }

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
      cur.asObjectCursor.right.map(_.value.toConfig).flatMap { config =>
        val strippedConfig = config.withoutPath("type")

        config.getString("type").toLowerCase match {
          case "linear" => ConfigReader[RecoveryDelayHandlers.Linear].from(strippedConfig.root())
          case "exponential" => ConfigReader[RecoveryDelayHandlers.Exponential].from(strippedConfig.root())
        }
      }
    }
  }

  private object StrictConnectionConfigReader extends ConfigReader[RabbitMQConnectionConfig] {
    private val DerivedReader: ConfigReader[RabbitMQConnectionConfig] = deriveReader[RabbitMQConnectionConfig]

    override def from(cur: ConfigCursor): Result[RabbitMQConnectionConfig] = {
      cur.asObjectCursor.right.map(_.value.toConfig).flatMap { config =>
        val configKeys = config.entrySet.asScala.map(_.getKey.split("\\.").head).toSet
        val classKeys = fieldsOf[RabbitMQConnectionConfig].map(_.name.toString).map(CamelCase.toTokens).map(namingConvention.fromTokens)

        // forbidden keys = all that are NOT in the case class and are NOT allowed
        // producer/consumer keys = all that contains "producer" or "consumer" in their name
        val forbiddenProducerConsumerKeys = (configKeys -- classKeys -- AllowedRootConfigKeys).filter { k =>
          val lowerCase = k.toLowerCase
          lowerCase.contains("consumer") || lowerCase.contains("producer") || lowerCase.contains("declar") || lowerCase.contains("bind")
        }.toList

        NonEmptyList.fromList(forbiddenProducerConsumerKeys) match {
          case Some(forbiddenProducerConsumerKeys) =>
            import cats.instances.either._
            import cats.syntax.traverse._

            forbiddenProducerConsumerKeys.map(cur.fluent.at(_).cursor).sequence.flatMap { cursors =>
              // generate error for all found keys, which are probably just misplaced
              val failures = cursors.map(c => ConvertFailure(UnknownProducerConsumerKey(c.path), c.location, c.path))

              // remove allowed keys and keys that are being already reported
              val strippedConfig = (AllowedRootConfigKeys ++ forbiddenProducerConsumerKeys.toList).foldLeft(config)(_.withoutPath(_))

              // add rest of failures which might raise when converting the rest of config to the case class
              val allFailures = failures ++ (DerivedReader.from(strippedConfig.root()) match {
                case Left(errs: ConfigReaderFailures) => errs.toList
                case _ => List.empty
              })

              Left(ConfigReaderFailures(allFailures.head, allFailures.tail))
            }

          case None =>
            // remove allowed keys
            val strippedConfig = AllowedRootConfigKeys.foldLeft(config)(_.withoutPath(_))
            DerivedReader.from(strippedConfig.root())
        }
      }
    }
  }
}

object PureconfigImplicits {
  private val AllowedRootConfigKeys = Set(ConsumersRootName, ProducersRootName, DeclarationsRootName)

  import scala.reflect.runtime.universe._

  private def fieldsOf[T: TypeTag]: List[MethodSymbol] =
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

  final case class UnknownProducerConsumerKey(key: String) extends FailureReason {
    val description: String = s"Unknown key. Maybe you forgot to move it to ${AllowedRootConfigKeys.mkString("/")} block?"
  }
}

sealed trait ConfigurationCheckPolicy

object ConfigurationCheckPolicy {
  case object Strict extends ConfigurationCheckPolicy
  case object NonStrict extends ConfigurationCheckPolicy
}
