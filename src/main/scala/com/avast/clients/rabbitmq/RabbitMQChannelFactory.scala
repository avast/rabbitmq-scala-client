package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.ExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.utils2.ssl.{KeyStoreTypes, SSLBuilder}
import com.rabbitmq.client.{Channel, Consumer, TopologyRecoveryException, _}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.util.control.NonFatal

trait RabbitMQChannelFactory {
  def createChannel(): ServerChannel
}

object RabbitMQChannelFactory {
  type ServerConnection = Connection with Recoverable
  type ServerChannel = Channel with Recoverable

  private[rabbitmq] final val RootConfigKey = "ffRabbitMQConnectionDefaults"

  private[rabbitmq] final val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = new ValueReader[Duration] {
    override def read(config: Config, path: String): Duration = config.getDuration(path)
  }

  private implicit final val JavaPathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }

  def fromConfig(providedConfig: Config, executor: Option[ExecutorService] = None): RabbitMQChannelFactory = {
    // we need to wrap it with one level, to be able to parse it with Ficus
    val config = ConfigFactory.empty()
      .withValue("root", providedConfig.withFallback(DefaultConfig).root())

    val connectionConfig = config.as[RabbitMQConnectionConfig]("root")

    val connection = createConnection(connectionConfig, executor)

    new RabbitMQChannelFactory {
      override def createChannel(): ServerChannel = {
        connection.createChannel() match {
          case c: ServerChannel => c

          // since the connection is `Recoverable`, the channel should always be `Recoverable` too (based on docs), so the exception will never be thrown
          case _ => throw new IllegalStateException(s"Required Recoverable Channel")
        }
      }
    }
  }

  protected def createConnection(config: RabbitMQConnectionConfig, executor: Option[ExecutorService]): ServerConnection = {
    import config._

    val factory = new ConnectionFactory
    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(true)
    factory.setNetworkRecoveryInterval(5000)
    factory.setRequestedHeartbeat(heartBeatInterval.getSeconds.toInt)

    factory.setExceptionHandler(UncaughtExceptionHandler)

    executor.foreach(factory.setSharedExecutor)

    if (credentials.enabled) {
      import credentials._

      factory.setUsername(username)
      factory.setPassword(password)
    }

    if (ssl.enabled) {
      import ssl.trustStore._

      if (ssl.trustStore.path.toString.trim.nonEmpty) {
        val sslContext = SSLBuilder.empty()
          .loadAllFromBundle(path, KeyStoreTypes.JKSTrustStore, password)
          .build

        factory.useSslProtocol(sslContext)
      } else {
        factory.useSslProtocol()
      }
    }

    factory.setConnectionTimeout(connectionTimeout.toMillis.toInt)

    val addresses = try {
      hosts.map(Address.parseAddress)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
    }

    factory.newConnection(addresses) match {
      case c: ServerConnection => c
      // since we set `factory.setAutomaticRecoveryEnabled(true)` it should always be `Recoverable` (based on docs), so the exception will never be thrown
      case _ => throw new IllegalStateException("Required Recoverable Connection")
    }
  }

  private[rabbitmq] object UncaughtExceptionHandler extends ExceptionHandler with LazyLogging {
    override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = {
      logger.error("Unexpected connection driver exception, closing the connection", exception)
      conn.abort()
    }

    override def handleConsumerException(channel: Channel, exception: Throwable, consumer: Consumer, consumerTag: String, methodName: String): Unit = {
      logger.warn(s"Error in consumer $consumerTag (while calling $methodName)", exception)
    }

    override def handleBlockedListenerException(connection: Connection, exception: Throwable): Unit = {
      logger.error("Unexpected blocked listener exception, closing the connection", exception)
      connection.abort()
    }

    override def handleFlowListenerException(channel: Channel, exception: Throwable): Unit = {
      logger.error("Unexpected flow listener exception", exception)
    }

    override def handleReturnListenerException(channel: Channel, exception: Throwable): Unit = {
      logger.error("Unexpected return listener exception", exception)
    }

    override def handleConfirmListenerException(channel: Channel, exception: Throwable): Unit = {
      logger.error("Unexpected confirm listener exception", exception)
    }

    override def handleTopologyRecoveryException(conn: Connection, ch: Channel, exception: TopologyRecoveryException): Unit = {
      logger.warn(s"Error in topology recovery to ${conn.getAddress}", exception)
    }

    override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = {
      logger.warn(s"Error in channel recovery (of connection to ${ch.getConnection})", exception)
    }

    override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = {
      logger.warn(s"Error in connection recovery to ${conn.getAddress}", exception)
    }
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
