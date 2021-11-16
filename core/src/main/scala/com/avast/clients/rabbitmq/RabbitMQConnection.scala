package com.avast.clients.rabbitmq

import java.io.IOException
import java.util
import java.util.concurrent.ExecutorService

import cats.effect._
import cats.syntax.functor._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client._
import com.typesafe.scalalogging.StrictLogging
import javax.net.ssl.SSLContext

import scala.collection.immutable

import scala.util.control.NonFatal

trait RabbitMQConnection[F[_]] {

  /** Creates new channel inside this connection. Usable for some applications-specific actions which are not supported by the library.<br>
    * The caller is responsible for closing the created channel - it's closed automatically only when the whole connection is closed.
    */
  def newChannel(): Resource[F, ServerChannel]

  /** Creates new instance of consumer, using the passed configuration.
    *
    * @param consumerConfig Configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed F.
    */
  def newConsumer[A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      monitor: Monitor,
      middlewares: List[RabbitMQConsumerMiddleware[F, A]] = Nil)(readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]]

  /** Creates new instance of producer, using the passed configuration.
    *
    * @param producerConfig Configuration of the producer.
    * @param monitor    Monitor for metrics.
    */
  def newProducer[A: ProductConverter](producerConfig: ProducerConfig, monitor: Monitor): Resource[F, RabbitMQProducer[F, A]]

  /** Creates new instance of pull consumer, using the passed configuration.
    *
    * @param pullConsumerConfig Configuration of the consumer.
    * @param monitor    Monitor for metrics.
    */
  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig,
                                            monitor: Monitor): Resource[F, RabbitMQPullConsumer[F, A]]

  /** Creates new instance of streaming consumer, using the passed configuration.
    *
    * @param consumerConfig Configuration of the consumer.
    * @param monitor Monitor for metrics.
    */
  def newStreamingConsumer[A: DeliveryConverter](
      consumerConfig: StreamingConsumerConfig,
      monitor: Monitor,
      middlewares: List[RabbitMQStreamingConsumerMiddleware[F, A]] = Nil): Resource[F, RabbitMQStreamingConsumer[F, A]]

  def declareExchange(config: DeclareExchangeConfig): F[Unit]
  def declareQueue(config: DeclareQueueConfig): F[Unit]
  def bindExchange(config: BindExchangeConfig): F[Unit]
  def bindQueue(config: BindQueueConfig): F[Unit]

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

  def make[F[_]: ConcurrentEffect: Timer: ContextShift](
      connectionConfig: RabbitMQConnectionConfig,
      blockingExecutor: ExecutorService,
      sslContext: Option[SSLContext] = None,
      connectionListener: ConnectionListener = DefaultListeners.DefaultConnectionListener,
      channelListener: ChannelListener = DefaultListeners.DefaultChannelListener,
      consumerListener: ConsumerListener = DefaultListeners.DefaultConsumerListener): Resource[F, RabbitMQConnection[F]] = {
    val connectionInfo = RabbitMQConnectionInfo(
      hosts = connectionConfig.hosts.toVector,
      virtualHost = connectionConfig.virtualHost,
      username = if (connectionConfig.credentials.enabled) Option(connectionConfig.credentials.username) else None
    )

    createConnection(connectionConfig, connectionInfo, blockingExecutor, sslContext, connectionListener, channelListener, consumerListener)
      .evalMap { connection =>
        val blocker = Blocker.liftExecutorService(blockingExecutor)

        DefaultRabbitMQConnection
          .make(
            connection = connection,
            info = connectionInfo,
            connectionListener = connectionListener,
            channelListener = channelListener,
            consumerListener = consumerListener,
            blocker = blocker,
            republishStrategy = connectionConfig.republishStrategy
          )
          .map(identity[RabbitMQConnection[F]]) // sad story about type inference
      }
  }

  protected def createConnection[F[_]: Sync](connectionConfig: RabbitMQConnectionConfig,
                                             connectionInfo: RabbitMQConnectionInfo,
                                             executor: ExecutorService,
                                             sslContext: Option[SSLContext],
                                             connectionListener: ConnectionListener,
                                             channelListener: ChannelListener,
                                             consumerListener: ConsumerListener): Resource[F, ServerConnection] =
    Resource.make {
      Sync[F].delay {
        import connectionConfig._

        val factory = createConnectionFactory(addressResolverType)
        val exceptionHandler = createExceptionHandler(connectionListener, channelListener, consumerListener)

        setUpConnection(connectionConfig, factory, exceptionHandler, sslContext, executor)

        val addresses = try {
          hosts.map(Address.parseAddress)
        } catch {
          case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
        }

        logger.info(s"Connecting to ${hosts.mkString("[", ", ", "]")}, virtual host '$virtualHost'")

        try {
          factory.newConnection(addresses.toArray, name) match {
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
    }(c => Sync[F].delay(c.close()))

  private def createConnectionFactory[F[_]: Sync](addressResolverType: AddressResolverType): ConnectionFactory = {
    import com.avast.clients.rabbitmq.AddressResolverType._

    new ConnectionFactory {
      override def createAddressResolver(addresses: util.List[Address]): AddressResolver = addressResolverType match {
        case Default => super.createAddressResolver(addresses)
        case List => new ListAddressResolver(addresses)
        case DnsRecord if addresses.size() == 1 => new DnsRecordIpAddressResolver(addresses.get(0))
        case DnsRecord => throw new IllegalArgumentException(s"DnsRecord configured but more hosts specified")
        case DnsSrvRecord if addresses.size() == 1 => new DnsSrvRecordAddressResolver(addresses.get(0).getHost)
        case DnsSrvRecord => throw new IllegalArgumentException(s"DnsSrvRecord configured but more hosts specified")
      }
    }
  }

  private def setUpConnection(connectionConfig: RabbitMQConnectionConfig,
                              factory: ConnectionFactory,
                              exceptionHandler: ExceptionHandler,
                              sslContext: Option[SSLContext],
                              executor: ExecutorService): Unit = {
    import connectionConfig._

    factory.setVirtualHost(virtualHost)

    factory.setTopologyRecoveryEnabled(topologyRecovery)
    factory.setAutomaticRecoveryEnabled(networkRecovery.enabled)
    factory.setExceptionHandler(exceptionHandler)
    factory.setRequestedHeartbeat(heartBeatInterval.toSeconds.toInt)

    if (networkRecovery.enabled) factory.setRecoveryDelayHandler(networkRecovery.handler)

    factory.setSharedExecutor(executor)

    if (credentials.enabled) {
      import credentials._

      factory.setUsername(username)
      factory.setPassword(password)
    }

    sslContext.foreach { sslContext =>
      factory.useSslProtocol(sslContext)
    }

    factory.setConnectionTimeout(connectionTimeout.toMillis.toInt)
  }

  // scalastyle:off
  private def createExceptionHandler(connectionListener: ConnectionListener,
                                     channelListener: ChannelListener,
                                     consumerListener: ConsumerListener): ExceptionHandler with RecoveryListener =
    new ExceptionHandler with RecoveryListener {
      override def handleReturnListenerException(channel: Channel, exception: Throwable): Unit = {
        logger.info(
          s"Return listener error on channel $channel (on connection ${channel.getConnection}, name ${channel.getConnection.getClientProvidedName})",
          exception
        )
      }

      override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on connection $conn (name ${conn.getClientProvidedName})", exception)
        connectionListener.onRecoveryFailure(conn, exception)
      }

      override def handleBlockedListenerException(conn: Connection, exception: Throwable): Unit = {
        logger.info(s"Recovery error on connection $conn (name ${conn.getClientProvidedName})", exception)
      }

      override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = {
        logger.debug(s"Recovery error on channel $ch", exception)
        channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = {
        exception match {
          case ioe: IOException => logger.info(s"RabbitMQ IO exception on $conn (name ${conn.getClientProvidedName})", ioe)
          case e => logger.debug(s"RabbitMQ unexpected exception on $conn (name ${conn.getClientProvidedName})", e)
        }
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
        logger.debug(s"Topology recovery error on channel $ch (on connection $conn, name ${conn.getClientProvidedName})", exception)
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

private[rabbitmq] case class RabbitMQConnectionInfo(hosts: immutable.Seq[String], virtualHost: String, username: Option[String])
