package com.avast.clients.rabbitmq

import java.net.{SocketException, UnknownHostException}
import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.ExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.{ChannelListener, ConnectionListener, ConsumerListener}
import com.avast.utils2.ssl.{KeyStoreTypes, SSLBuilder}
import com.rabbitmq.client.{Channel, _}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader
import net.jodah.lyra.config.{ConfigurableConnection, RecoveryPolicies, RetryPolicies, Config => LyraConfig}
import net.jodah.lyra.util.{Duration => LyraDuration}
import net.jodah.lyra.{ConnectionOptions, Connections}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.language.implicitConversions
import scala.util.control.NonFatal

trait RabbitMQChannelFactory {
  def createChannel(): ServerChannel

  def info: RabbitMqChannelFactoryInfo

  def connectionListener: ConnectionListener
  def channelListener: ChannelListener
  def consumerListener: ConsumerListener
}

object RabbitMQChannelFactory extends StrictLogging {
  type ServerConnection = ConfigurableConnection
  type ServerChannel = Channel

  object DefaultListeners {
    final val DefaultConnectionListener: ConnectionListener = new net.jodah.lyra.event.DefaultConnectionListener with ConnectionListener {}
    final val DefaultChannelListener: ChannelListener = new net.jodah.lyra.event.DefaultChannelListener with ChannelListener {
      override def onShutdown(cause: ShutdownSignalException, channel: Channel): Unit = ()
    }
    final val DefaultConsumerListener: ConsumerListener = new net.jodah.lyra.event.DefaultConsumerListener with ConsumerListener {
      override def onError(consumer: Consumer, channel: ServerChannel, failure: Throwable): Unit = ()
      override def onShutdown(consumer: Consumer, channel: Channel, consumerTag: String, sig: ShutdownSignalException): Unit = ()
    }
  }

  private object Exceptions {
    private[RabbitMQChannelFactory] final val RecoverableExceptions = Set(classOf[UnknownHostException], classOf[SocketException]).asJava
    private[RabbitMQChannelFactory] final val RetryableExceptions = Set(classOf[UnknownHostException], classOf[SocketException]).asJava
  }

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
                 connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
                 channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
                 consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener): RabbitMQChannelFactory = {
    // we need to wrap it with one level, to be able to parse it with Ficus
    val config = ConfigFactory
      .empty()
      .withValue("root", providedConfig.withFallback(DefaultConfig).root())

    val connectionConfig = config.as[RabbitMQConnectionConfig]("root")

    create(connectionConfig, executor, connectionListener, channelListener, consumerListener)
  }

  def create(connectionConfig: RabbitMQConnectionConfig,
             executor: Option[ExecutorService] = None,
             connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
             channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
             consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener): RabbitMQChannelFactory = {

    val connection = createConnection(connectionConfig, executor, connectionListener, channelListener, consumerListener)

    // temporary variables which are used as a bridge between variables with same names
    val connListener = connectionListener
    val chanListener = channelListener
    val consListener = consumerListener

    new RabbitMQChannelFactory {

      override val info: RabbitMqChannelFactoryInfo = RabbitMqChannelFactoryInfo(
        hosts = connectionConfig.hosts.toVector,
        virtualHost = connectionConfig.virtualHost
      )

      override def createChannel(): ServerChannel = {
        val channel = connection.createChannel()
        channel.addShutdownListener(new ShutdownListener {
          override def shutdownCompleted(cause: ShutdownSignalException): Unit = chanListener.onShutdown(cause, channel)
        })
        channel
      }

      override val connectionListener: ConnectionListener = connListener

      override val channelListener: ChannelListener = chanListener

      override val consumerListener: ConsumerListener = consListener
    }
  }

  private implicit def javaDurationToLyraDuration(d: Duration): LyraDuration = {
    LyraDuration.millis(d.toMillis)
  }

  protected def createConnection(connectionConfig: RabbitMQConnectionConfig,
                                 executor: Option[ExecutorService],
                                 connectionListener: ConnectionListener,
                                 channelListener: ChannelListener,
                                 consumerListener: ConsumerListener): ServerConnection = {
    import connectionConfig._

    val factory = new ConnectionFactory
    setUpConnection(connectionConfig, factory, executor)

    val addresses = try {
      hosts.map(Address.parseAddress)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
    }

    logger.info(s"Connecting to ${hosts.mkString("[", ", ", "]")}, virtual host '$virtualHost'")

    val connectionOptions = new ConnectionOptions(factory)
      .withAddresses(addresses: _*)
      .withName(name)
      .withClientProperties(Map("connection_name" -> name.asInstanceOf[AnyRef]).asJava)
    // this is workaround of Lyras bug in ConnectionHandler:243

    val lyraConfig = new LyraConfig()
      .withConnectionListeners(connectionListener)
      .withChannelListeners(channelListener)
      .withConsumerListeners(consumerListener)

    if (networkRecovery.enabled) {
      lyraConfig
        .withRecoveryPolicy(RecoveryPolicies.recoverAlways().withInterval(networkRecovery.period))
        .withRetryPolicy(RetryPolicies.retryAlways().withInterval(networkRecovery.period))
    }

    lyraConfig.getRecoverableExceptions.addAll(Exceptions.RecoverableExceptions)
    lyraConfig.getRetryableExceptions.addAll(Exceptions.RetryableExceptions)

    Connections.create(connectionOptions, lyraConfig)
  }

  private def setUpConnection(connectionConfig: RabbitMQConnectionConfig,
                              factory: ConnectionFactory,
                              executor: Option[ExecutorService]): Unit = {
    import connectionConfig._

    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(false) // Lyra will handle the recovery
    factory.setRequestedHeartbeat(heartBeatInterval.getSeconds.toInt)

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

case class RabbitMqChannelFactoryInfo(hosts: immutable.Seq[String], virtualHost: String)
