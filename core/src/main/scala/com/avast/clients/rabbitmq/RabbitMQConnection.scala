package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util.concurrent.ExecutorService

import cats.effect.Effect
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.ssl.{KeyStoreTypes, SSLBuilder}
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import javax.net.ssl.SSLContext
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

trait RabbitMQConnection[F[_]] extends AutoCloseable {

  /** Creates new channel inside this connection. Usable for some applications-specific actions which are not supported by the library.<br>
    * The caller is responsible for closing the created channel - it's closed automatically only when the whole connection is closed.
    */
  def newChannel(): ServerChannel

  /** Creates new instance of consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed future.
    */
  def newConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(readAction: DeliveryReadAction[F, A])(
      implicit ec: ExecutionContext): RabbitMQConsumer with AutoCloseable

  /** Creates new instance of producer, using the TypeSafe configuration passed to the factory and producer name.
    *
    * @param configName Name of configuration of the producer.
    * @param monitor    Monitor for metrics.F
    */
  def newProducer[A: ProductConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): RabbitMQProducer[F, A] with AutoCloseable

  /** Creates new instance of pull consumer, using the TypeSafe configuration passed to the factory and consumer name.
    *
    * @param configName Name of configuration of the consumer.
    * @param monitor    Monitor for metrics.
    */
  def newPullConsumer[A: DeliveryConverter](configName: String, monitor: Monitor)(
      implicit ec: ExecutionContext): RabbitMQPullConsumer[F, A] with AutoCloseable

  /**
    * Declares and additional exchange, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareExchange(configName: String): F[Unit]

  /**
    * Declares and additional queue, using the TypeSafe configuration passed to the factory and config name.
    */
  def declareQueue(configName: String): F[Unit]

  /**
    * Binds a queue to an exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindQueue(configName: String): F[Unit]

  /**
    * Binds an exchange to an another exchange, using the TypeSafe configuration passed to the factory and config name.<br>
    * Failure indicates that the binding has failed for AT LEAST one routing key.
    */
  def bindExchange(configName: String): F[Unit]

  /** Executes a specified action with newly created [[ServerChannel]] which is then closed.
    *
    * @see #newChannel()
    * @return Result of performed action.
    */
  def withChannel[A](f: ServerChannel => F[A]): F[A]

  def connectionListener: ConnectionListener
  def channelListener: ChannelListener
  def consumerListener: ConsumerListener
}

object RabbitMQConnection extends StrictLogging {

  object DefaultListeners {
    final val DefaultConnectionListener: ConnectionListener = ConnectionListener.Default
    final val DefaultChannelListener: ChannelListener = ChannelListener.Default
    final val DefaultConsumerListener: ConsumerListener = ConsumerListener.Default
  }

  private[rabbitmq] final val RootConfigKey = "avastRabbitMQConnectionDefaults"

  private[rabbitmq] final val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = (config: Config, path: String) => config.getDuration(path)

  private implicit final val JavaPathReader: ValueReader[Path] = (config: Config, path: String) => Paths.get(config.getString(path))

  /** Creates new instance of channel factory, using the passed configuration.
    *
    * @param providedConfig   The configuration.
    * @param blockingExecutor [[ExecutorService]] which should be used as shared blocking pool (IO operations) for all channels from this connection.
    */
  def fromConfig[F[_]: Effect](
      providedConfig: Config,
      blockingExecutor: ExecutorService,
      connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
      channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
      consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener): DefaultRabbitMQConnection[F] = {
    // we need to wrap it with one level, to be able to parse it with Ficus
    val config = ConfigFactory
      .empty()
      .withValue("root", providedConfig.withFallback(DefaultConfig).root())

    val connectionConfig = config.as[RabbitMQConnectionConfig]("root")

    val connection = createConnection(connectionConfig, blockingExecutor, connectionListener, channelListener, consumerListener)

    val blockingScheduler: Scheduler = Scheduler(ses, ExecutionContext.fromExecutor(blockingExecutor))

    new DefaultRabbitMQConnection(
      connection = connection,
      info = RabbitMQConnectionInfo(
        hosts = connectionConfig.hosts.toVector,
        virtualHost = connectionConfig.virtualHost
      ),
      config = providedConfig,
      connectionListener = connectionListener,
      channelListener = channelListener,
      consumerListener = consumerListener,
      blockingScheduler = blockingScheduler
    )
  }

  protected def createConnection(connectionConfig: RabbitMQConnectionConfig,
                                 executor: ExecutorService,
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
                              executor: ExecutorService): Unit = {
    import connectionConfig._

    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(true)
    factory.setNetworkRecoveryInterval(networkRecovery.period.toMillis)
    factory.setExceptionHandler(exceptionHandler)
    factory.setRequestedHeartbeat(heartBeatInterval.getSeconds.toInt)

    factory.setSharedExecutor(executor)

    if (credentials.enabled) {
      import credentials._

      factory.setUsername(username)
      factory.setPassword(password)
    }

    if (connectionConfig.ssl.enabled) {
      import connectionConfig.ssl.trustStore._

      if (connectionConfig.ssl.trustStore.path.toString.trim.nonEmpty) {
        val sslContext = SSLBuilder
          .empty()
          .loadAllFromBundle(path, KeyStoreTypes.JKSTrustStore, password)
          .build

        factory.useSslProtocol(sslContext)
      } else {
        factory.useSslProtocol(SSLContext.getDefault)
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
        logger.info(s"Return listener error on channel $channel", exception)
      }

      override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on connection $conn", exception)
        connectionListener.onRecoveryFailure(conn, exception)
      }

      override def handleBlockedListenerException(connection: Connection, exception: Throwable): Unit = {
        logger.info(s"Recovery error on connection $connection", exception)
      }

      override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on channel $ch", exception)
        channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = {
        logger.info("RabbitMQ driver exception", exception)
      }

      override def handleConsumerException(channel: Channel,
                                           exception: Throwable,
                                           consumer: Consumer,
                                           consumerTag: String,
                                           methodName: String): Unit = {
        logger.debug(s"Consumer exception on channel $channel, consumer with tag '$consumerTag', method '$methodName'")

        val consumerName = consumer match {
          case c: DefaultRabbitMQConsumer[_] => c.name
          case _ => "unknown"
        }

        consumerListener.onError(consumer, consumerName, channel, exception)
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
