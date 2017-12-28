package com.avast.clients.rabbitmq

import java.net.{SocketException, UnknownHostException}
import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.{ExecutorService, ScheduledExecutorService}

import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.avast.utils2.errorhandling.FutureTimeouter
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
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

trait RabbitMQFactory extends RabbitMQFactoryManual {

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[ExecutionContext]] used for callbacks.
    */
  def newConsumer(configName: String, monitor: Monitor)(readAction: Delivery => Future[DeliveryResult])(
      implicit ec: ExecutionContext): RabbitMQConsumer

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param monitor    Monitor for metrics.F
    */
  def newProducer(configName: String, monitor: Monitor): RabbitMQProducer

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareExchange(configName: String): Try[Done]

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareQueue(configName: String): Try[Done]

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindQueue(configName: String): Try[Done]

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindExchange(configName: String): Try[Done]
}

trait RabbitMQFactoryManual extends AutoCloseable {

  /** Creates new instance of consumer, using passed configuration.
    *
    * @param config     Configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    * @param ec         [[ExecutionContext]] used for callbacks.
    */
  def newConsumer(config: ConsumerConfig, monitor: Monitor)(readAction: Delivery => Future[DeliveryResult])(
      implicit ec: ExecutionContext): RabbitMQConsumer

  /** Creates new instance of producer, using passed configuration.
    *
    * @param config  Configuration of the producer.
    * @param monitor Monitor for metrics.
    */
  def newProducer(config: ProducerConfig, monitor: Monitor): RabbitMQProducer

  /**
    * Declares and additional exchange, using passed configuration.
    */
  def declareExchange(config: DeclareExchange): Try[Done]

  /**
    * Declares and additional queue, using passed configuration.
    */
  def declareQueue(config: DeclareQueue): Try[Done]

  /**
    * Binds a queue to an exchange, using passed configuration.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindQueue(config: BindQueue): Try[Done]

  /**
    * Binds an exchange to an another exchange, using passed configuration.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindExchange(config: BindExchange): Try[Done]

  /** Closes this factory and all created consumers and producers.
    */
  override def close(): Unit
}

object RabbitMQFactory extends StrictLogging {
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
    private[RabbitMQFactory] final val RecoverableExceptions = Set(classOf[UnknownHostException], classOf[SocketException]).asJava
    private[RabbitMQFactory] final val RetryableExceptions = Set(classOf[UnknownHostException], classOf[SocketException]).asJava
  }

  private[rabbitmq] final val RootConfigKey = "ffRabbitMQConnectionDefaults"

  private[rabbitmq] final val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = (config: Config, path: String) => config.getDuration(path)

  private implicit final val JavaPathReader: ValueReader[Path] = (config: Config, path: String) => Paths.get(config.getString(path))

  /** Creates new instance of channel factory, using the passed configuration.
    *
    * @param providedConfig           The configuration.
    * @param executor                 [[ExecutorService]] which should be used as shared for all channels from this factory. Optional parameter.
    * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks for all created consumers.
    */
  def fromConfig(
      providedConfig: Config,
      executor: Option[ExecutorService] = None,
      connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
      channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
      consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener,
      scheduledExecutorService: ScheduledExecutorService = FutureTimeouter.Implicits.DefaultScheduledExecutor): RabbitMQFactory = {
    // we need to wrap it with one level, to be able to parse it with Ficus
    val config = ConfigFactory
      .empty()
      .withValue("root", providedConfig.withFallback(DefaultConfig).root())

    val connectionConfig = config.as[RabbitMQConnectionConfig]("root")

    create(connectionConfig, providedConfig, executor, connectionListener, channelListener, consumerListener, scheduledExecutorService)
  }

  /** Creates new instance of channel factory, using the passed configuration.
    *
    * @param connectionConfig         The configuration.
    * @param executor                 [[ExecutorService]] which should be used as shared for all channels from this factory. Optional parameter.
    * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks for all created consumers.
    */
  def create(
      connectionConfig: RabbitMQConnectionConfig,
      executor: Option[ExecutorService] = None,
      connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
      channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
      consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener,
      scheduledExecutorService: ScheduledExecutorService = FutureTimeouter.Implicits.DefaultScheduledExecutor): RabbitMQFactoryManual = {

    create(connectionConfig,
           ConfigFactory.empty(),
           executor,
           connectionListener,
           channelListener,
           consumerListener,
           scheduledExecutorService)
  }

  private def create(connectionConfig: RabbitMQConnectionConfig,
                     providedConfig: Config,
                     executor: Option[ExecutorService],
                     connectionListener: ConnectionListener,
                     channelListener: ChannelListener,
                     consumerListener: ConsumerListener,
                     scheduledExecutorService: ScheduledExecutorService): RabbitMQFactory = {

    val connection = createConnection(connectionConfig, executor, connectionListener, channelListener, consumerListener)

    new RabbitMQFactoryImpl(
      connection = connection,
      info = RabbitMqFactoryInfo(
        hosts = connectionConfig.hosts.toVector,
        virtualHost = connectionConfig.virtualHost
      ),
      config = providedConfig,
      connectionListener = connectionListener,
      channelListener = channelListener,
      consumerListener = consumerListener,
      scheduledExecutorService
    )
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
