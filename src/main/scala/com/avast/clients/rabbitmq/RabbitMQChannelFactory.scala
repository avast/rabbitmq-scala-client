package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.ExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.utils2.ssl.{KeyStoreTypes, SSLBuilder}
import com.rabbitmq.client.{Channel, _}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.immutable
import scala.util.control.NonFatal

trait RabbitMQChannelFactory {
  def createChannel(): ServerChannel

  def info: RabbitMqChannelFactoryInfo
}

object RabbitMQChannelFactory extends StrictLogging {
  type ServerConnection = Connection with Recoverable
  type ServerChannel = Channel with Recoverable

  final lazy val DefaultExceptionHandler: ExceptionHandler = new LoggingUncaughtExceptionHandler

  private[rabbitmq] final val RootConfigKey = "ffRabbitMQConnectionDefaults"

  private[rabbitmq] final val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = new ValueReader[Duration] {
    override def read(config: Config, path: String): Duration = config.getDuration(path)
  }

  private implicit final val JavaPathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }

  /** Creates new instance of channel factory, using the passed configuration.
    *
    * @param providedConfig The configuration.
    * @param executor       [[ExecutorService]] which should be used as shared for all channels from this factory. Optional parameter.
    */
  def fromConfig(providedConfig: Config,
                 executor: Option[ExecutorService] = None,
                 exceptionHandler: ExceptionHandler = DefaultExceptionHandler): RabbitMQChannelFactory = {
    // we need to wrap it with one level, to be able to parse it with Ficus
    val config = ConfigFactory
      .empty()
      .withValue("root", providedConfig.withFallback(DefaultConfig).root())

    val connectionConfig = config.as[RabbitMQConnectionConfig]("root")

    create(connectionConfig, executor, exceptionHandler)
  }

  def create(connectionConfig: RabbitMQConnectionConfig,
             executor: Option[ExecutorService] = None,
             exceptionHandler: ExceptionHandler = DefaultExceptionHandler): RabbitMQChannelFactory = {

    val connection = createConnection(connectionConfig, executor, exceptionHandler)

    new RabbitMQChannelFactory {

      override val info: RabbitMqChannelFactoryInfo = RabbitMqChannelFactoryInfo(
        hosts = connectionConfig.hosts.toVector,
        virtualHost = connectionConfig.virtualHost
      )

      override def createChannel(): ServerChannel = {
        connection.createChannel() match {
          case c: ServerChannel => c

          // since the connection is `Recoverable`, the channel should always be `Recoverable` too (based on docs), so the exception will never be thrown
          case _ => throw new IllegalStateException(s"Required Recoverable Channel")
        }
      }
    }
  }

  protected def createConnection(config: RabbitMQConnectionConfig,
                                 executor: Option[ExecutorService],
                                 exceptionHandler: ExceptionHandler): ServerConnection = {
    import config._

    val factory = new ConnectionFactory
    setUpConnection(config, factory, executor, exceptionHandler)

    val addresses = try {
      hosts.map(Address.parseAddress)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
    }

    logger.info(s"Connecting to ${hosts.mkString("[", ", ", "]")}, virtual host '$virtualHost'")

    factory.newConnection(addresses) match {
      case c: ServerConnection => c
      // since we set `factory.setAutomaticRecoveryEnabled(true)` it should always be `Recoverable` (based on docs), so the exception will never be thrown
      case _ => throw new IllegalStateException("Required Recoverable Connection")
    }
  }

  private def setUpConnection(connectionConfig: RabbitMQConnectionConfig,
                              factory: ConnectionFactory,
                              executor: Option[ExecutorService],
                              exceptionHandler: ExceptionHandler): Unit = {
    import connectionConfig._

    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(true)
    factory.setNetworkRecoveryInterval(5000)
    factory.setRequestedHeartbeat(heartBeatInterval.getSeconds.toInt)

    factory.setExceptionHandler(exceptionHandler)

    executor.foreach(factory.setSharedExecutor)

    if (credentials.enabled) {
      import credentials._

      factory.setUsername(username)
      factory.setPassword(password)
    }

    if (ssl.enabled) {
      import ssl.trustStore._

      if (ssl.trustStore.path.toString.trim.nonEmpty) {
        val sslContext = SSLBuilder
          .empty()
          .loadAllFromBundle(path, KeyStoreTypes.JKSTrustStore, password)
          .build

        factory.useSslProtocol(sslContext)
      } else {
        factory.useSslProtocol()
      }
    }

    factory.setConnectionTimeout(connectionTimeout.toMillis.toInt)
  }

}

case class RabbitMQConnectionConfig(hosts: Array[String],
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

case class RabbitMqChannelFactoryInfo(hosts: immutable.Seq[String], virtualHost: String)
