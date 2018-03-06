package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.{ExecutorService, ScheduledExecutorService}

import com.avast.clients.rabbitmq.api.{Delivery, _}
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.avast.utils2.errorhandling.FutureTimeouter
import com.avast.utils2.ssl.{KeyStoreTypes, SSLBuilder}
import com.rabbitmq.client._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
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
  type ServerConnection = RecoverableConnection
  type ServerChannel = RecoverableChannel

  object DefaultListeners {
    final val DefaultConnectionListener: ConnectionListener = ConnectionListener.Default
    final val DefaultChannelListener: ChannelListener = ChannelListener.Default
    final val DefaultConsumerListener: ConsumerListener = ConsumerListener.Default
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

  protected def createConnection(connectionConfig: RabbitMQConnectionConfig,
                                 executor: Option[ExecutorService],
                                 connectionListener: ConnectionListener,
                                 channelListener: ChannelListener,
                                 consumerListener: ConsumerListener): ServerConnection = {
    import connectionConfig._

    val factory = new ConnectionFactory
    val exceptionHandler = createExceptionHandler(connectionListener, channelListener, consumerListener)
    setUpConnection(connectionConfig, factory, exceptionHandler, executor)

    val addresses = try {
      hosts.map(Address.parseAddress)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
    }

    logger.info(s"Connecting to ${hosts.mkString("[", ", ", "]")}, virtual host '$virtualHost'")

    try {
      factory.newConnection(addresses, name) match {
        case conn: ServerConnection =>
          conn.addRecoveryListener(exceptionHandler)
          conn.addShutdownListener((cause: ShutdownSignalException) => connectionListener.onShutdown(conn, cause))
          connectionListener.onCreate(conn)
          conn
        // since we set `factory.setAutomaticRecoveryEnabled(true)` it should always be `Recoverable` (based on docs), so the exception will never be thrown
        case _ => throw new IllegalStateException("Required Recoverable Connection")
      }
    } catch {
      case NonFatal(e) =>
        connectionListener.onCreateFailure(e)
        throw e
    }
  }

  private def setUpConnection(connectionConfig: RabbitMQConnectionConfig,
                              factory: ConnectionFactory,
                              exceptionHandler: ExceptionHandler,
                              executor: Option[ExecutorService]): Unit = {
    import connectionConfig._

    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(true)
    factory.setNetworkRecoveryInterval(networkRecovery.period.toMillis)
    factory.setExceptionHandler(exceptionHandler)
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

  // scalastyle:off
  private def createExceptionHandler(connectionListener: ConnectionListener,
                                     channelListener: ChannelListener,
                                     consumerListener: ConsumerListener): ExceptionHandler with RecoveryListener =
    new ExceptionHandler with RecoveryListener {
      override def handleReturnListenerException(channel: Channel, exception: Throwable): Unit = {
        logger.debug(s"Return listener error on channel $channel", exception)
      }

      override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on connection $conn", exception)
        connectionListener.onRecoveryFailure(conn, exception)
      }

      override def handleBlockedListenerException(connection: Connection, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on connection $connection", exception)
      }

      override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on channel $ch", exception)
        channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = {
        logger.info("RabbitMQ driver exception")
      }

      override def handleConsumerException(channel: Channel,
                                           exception: Throwable,
                                           consumer: Consumer,
                                           consumerTag: String,
                                           methodName: String): Unit = {
        logger.debug(s"Consumer exception on channel $channel, consumer with tag '$consumerTag', method '$methodName'")
        consumerListener.onError(consumer, channel, exception)
      }

      override def handleTopologyRecoveryException(conn: Connection, ch: Channel, exception: TopologyRecoveryException): Unit = {
        logger.debug(s"Topology recovery error on channel $ch (connection $conn)", exception)
        channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleConfirmListenerException(channel: Channel, exception: Throwable): Unit = {
        logger.debug(s"Confirm listener error on channel $channel", exception)
      }

      // recovery listener

      override def handleRecovery(recoverable: Recoverable): Unit = {
        logger.debug(s"Recovery completed on $recoverable")

        recoverable match {
          case ch: ServerChannel => channelListener.onRecoveryCompleted(ch)
          case conn: ServerConnection => connectionListener.onRecoveryCompleted(conn)
        }
      }

      override def handleRecoveryStarted(recoverable: Recoverable): Unit = {
        logger.debug(s"Recovery started on $recoverable")

        recoverable match {
          case ch: ServerChannel => channelListener.onRecoveryStarted(ch)
          case conn: ServerConnection => connectionListener.onRecoveryStarted(conn)
        }
      }
    }

}
