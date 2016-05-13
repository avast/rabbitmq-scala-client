package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.avast.clients.rabbitmq.api.RabbitMQSenderAndReceiver
import com.avast.metrics.api.Monitor
import com.avast.utils2.ssl.{KeyStoreTypes, SSLBuilder}
import com.rabbitmq.client.{Channel, Consumer, TopologyRecoveryException, _}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.ExecutionContextExecutorService
import scala.util.control.NonFatal


object RabbitMQClientFactory {
  private[rabbitmq] type ServerConnection = Connection with Recoverable
  private[rabbitmq] type ServerChannel = Channel with Recoverable

  private[rabbitmq] final val RootConfigKey = "ffRabbitMQBooMinderDefaults"

  private[rabbitmq] final val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = new ValueReader[Duration] {
    override def read(config: Config, path: String): Duration = config.getDuration(path)
  }

  private implicit final val JavaPathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }

  object SenderAndReceiver {
    def fromConfig(providedConfig: Config, monitor: Monitor, executor: ExecutionContextExecutorService): RabbitMQSenderAndReceiver = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      val config = ConfigFactory.empty()
        .withValue("root", providedConfig.withFallback(DefaultConfig).root())

      val rabbitConfig = config.as[RabbitMQClientSenderReceiverConfig]("root")

      val channel = connectAndCreateChannel(rabbitConfig, executor)

      ???
    }
  }

  protected def connectAndCreateChannel(rabbitConfig: RabbitMQClientConfig, executor: ExecutionContextExecutorService): ServerChannel = {
    import rabbitConfig._

    val factory = createConnectionFactory(rabbitConfig, executor)

    val addresses = try {
      hosts.map(Address.parseAddress)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
    }

    val connection: ServerConnection = factory.newConnection(addresses) match {
      case c: ServerConnection => c
      // since we set `factory.setAutomaticRecoveryEnabled(true)` it should always be `Recoverable` (based on docs), so the exception will never be thrown
      case _ => throw new IllegalStateException("Required Recoverable Connection")
    }

    val channel: ServerChannel = connection.createChannel() match {
      case c: ServerChannel =>
        c

      // since the connection is `Recoverable`, the channel should always be `Recoverable` too (based on docs), so the exception will never be thrown
      case _ => throw new IllegalStateException(s"Required Recoverable Channel")
    }

    channel
  }

  protected def createConnectionFactory(config: RabbitMQClientConfig, executor: ExecutionContextExecutorService): ConnectionFactory = {
    import config._

    val factory = new ConnectionFactory
    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(true)
    factory.setAutomaticRecoveryEnabled(true)
    factory.setNetworkRecoveryInterval(5000)
    factory.setRequestedHeartbeat(heartBeatInterval.getSeconds.toInt)

    factory.setExceptionHandler(UncaughtExceptionHandler)

    factory.setSharedExecutor(executor)

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

    factory
  }

  private object UncaughtExceptionHandler extends ExceptionHandler with LazyLogging {
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

sealed trait RabbitMQClientConfig {
  val hosts: Array[String]

  val virtualHost: String

  val exchange: String

  val connectionTimeout: Duration

  val heartBeatInterval: Duration

  val processTimeout: Duration

  val credentials: Credentials

  val ssl: Ssl

  val name: String
}

case class RabbitMQClientSenderReceiverConfig(hosts: Array[String],
                                              virtualHost: String,
                                              exchange: String,
                                              connectionTimeout: Duration,
                                              heartBeatInterval: Duration,
                                              processTimeout: Duration,
                                              credentials: Credentials,
                                              ssl: Ssl,
                                              receiver: Receiver,
                                              sender: Sender,
                                              name: String) extends RabbitMQClientConfig

case class RabbitMQClientReceiverConfig(hosts: Array[String],
                                        virtualHost: String,
                                        exchange: String,
                                        connectionTimeout: Duration,
                                        heartBeatInterval: Duration,
                                        processTimeout: Duration,
                                        credentials: Credentials,
                                        ssl: Ssl,
                                        receiver: Receiver,
                                        name: String) extends RabbitMQClientConfig

case class RabbitMQClientSenderConfig(hosts: Array[String],
                                      virtualHost: String,
                                      exchange: String,
                                      connectionTimeout: Duration,
                                      heartBeatInterval: Duration,
                                      processTimeout: Duration,
                                      credentials: Credentials,
                                      ssl: Ssl,
                                      sender: Sender,
                                      name: String) extends RabbitMQClientConfig

case class Receiver(queueName: String)

case class Sender(exchange: String)

case class Credentials(enabled: Boolean, username: String, password: String)

case class Ssl(enabled: Boolean, trustStore: TrustStore)

case class TrustStore(path: Path, password: String)
