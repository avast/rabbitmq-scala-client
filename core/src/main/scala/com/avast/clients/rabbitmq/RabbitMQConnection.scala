package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits.catsSyntaxFlatMapOps
import cats.syntax.functor._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client._

import java.io.IOException
import java.util
import java.util.concurrent.ExecutorService
import javax.net.ssl.SSLContext
import scala.collection.immutable
import scala.util.control.NonFatal

trait RabbitMQConnection[F[_]] {

  /** Creates new channel inside this connection. Usable for some applications-specific actions which are not supported by the library.
    */
  def newChannel(): Resource[F, ServerChannel]

  /** Creates new instance of consumer, using the passed configuration.
    *
    * @param consumerConfig Configuration of the consumer.
    * @param monitor    Monitor for metrics.
    * @param readAction Action executed for each delivered message. You should never return a failed F.
    */
  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig, monitor: Monitor)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]]

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
  def newStreamingConsumer[A: DeliveryConverter](consumerConfig: StreamingConsumerConfig,
                                                 monitor: Monitor): Resource[F, RabbitMQStreamingConsumer[F, A]]

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

  def connectionListener: ConnectionListener[F]
  def channelListener: ChannelListener[F]
  def consumerListener: ConsumerListener[F]
}

object RabbitMQConnection {

  object DefaultListeners {
    def defaultConnectionListener[F[_]: Sync]: ConnectionListener[F] = ConnectionListener.default[F]
    def defaultChannelListener[F[_]: Sync]: ChannelListener[F] = ChannelListener.default[F]
    def defaultConsumerListener[F[_]: Sync]: ConsumerListener[F] = ConsumerListener.default[F]
  }

  def make[F[_]: ConcurrentEffect: Timer: ContextShift](
      connectionConfig: RabbitMQConnectionConfig,
      blockingExecutor: ExecutorService,
      sslContext: Option[SSLContext] = None,
      connectionListener: Option[ConnectionListener[F]] = None,
      channelListener: Option[ChannelListener[F]] = None,
      consumerListener: Option[ConsumerListener[F]] = None): Resource[F, RabbitMQConnection[F]] = {
    val logger = ImplicitContextLogger.createLogger[F, RabbitMQConnection.type]
    val blocker = Blocker.liftExecutorService(blockingExecutor)

    val connectionInfo = RabbitMQConnectionInfo(
      hosts = connectionConfig.hosts.toVector,
      virtualHost = connectionConfig.virtualHost,
      username = if (connectionConfig.credentials.enabled) Option(connectionConfig.credentials.username) else None
    )

    val finalConnectionListener = connectionListener.getOrElse(ConnectionListener.default)
    val finalChannelListener = channelListener.getOrElse(ChannelListener.default)
    val finalConsumerListener = consumerListener.getOrElse(ConsumerListener.default)

    createConnection(connectionConfig,
                     blockingExecutor,
                     blocker,
                     sslContext,
                     finalConnectionListener,
                     finalChannelListener,
                     finalConsumerListener,
                     logger)
      .evalMap { connection =>
        DefaultRabbitMQConnection
          .make(
            connection = connection,
            info = connectionInfo,
            connectionListener = finalConnectionListener,
            channelListener = finalChannelListener,
            consumerListener = finalConsumerListener,
            blocker = blocker,
            republishStrategy = connectionConfig.republishStrategy
          )
          .map(identity[RabbitMQConnection[F]]) // sad story about type inference
      }
  }

  protected def createConnection[F[_]: Effect: ContextShift](connectionConfig: RabbitMQConnectionConfig,
                                                             executor: ExecutorService,
                                                             blocker: Blocker,
                                                             sslContext: Option[SSLContext],
                                                             connectionListener: ConnectionListener[F],
                                                             channelListener: ChannelListener[F],
                                                             consumerListener: ConsumerListener[F],
                                                             logger: ImplicitContextLogger[F]): Resource[F, ServerConnection] = {
    import connectionConfig._

    val factory = createConnectionFactory(addressResolverType)
    val exceptionHandler = createExceptionHandler[F](connectionListener, channelListener, consumerListener)

    Resource.make {
      setUpConnection(connectionConfig, factory, exceptionHandler, sslContext, executor, blocker) >>
        Sync[F].delay {
          try { hosts.map(Address.parseAddress) } catch {
            case NonFatal(e) => throw new IllegalArgumentException("Invalid format of hosts", e)
          }
        } >>= { addresses =>
        logger.plainInfo(s"Connecting to ${hosts.mkString("[", ", ", "]")}, virtual host '$virtualHost'") >>
          blocker.delay {
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
      }
    }(c => Sync[F].delay(c.close()))
  }

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

  private def setUpConnection[F[_]: Sync: ContextShift](connectionConfig: RabbitMQConnectionConfig,
                                                        factory: ConnectionFactory,
                                                        exceptionHandler: ExceptionHandler,
                                                        sslContext: Option[SSLContext],
                                                        executor: ExecutorService,
                                                        blocker: Blocker): F[Unit] = blocker.delay {
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
  private def createExceptionHandler[F[_]: Effect](connectionListener: ConnectionListener[F],
                                                   channelListener: ChannelListener[F],
                                                   consumerListener: ConsumerListener[F]): ExceptionHandler with RecoveryListener =
    new ExceptionHandler with RecoveryListener {
      private val logger = ImplicitContextLogger.createLogger[F, ExceptionHandler]

      private def async(f: F[Unit]): Unit = {
        Effect[F].toIO(f).unsafeToFuture()
      }

      override def handleReturnListenerException(channel: Channel, exception: Throwable): Unit = async {
        logger.plainInfo(exception)(
          s"Return listener error on channel $channel (on connection ${channel.getConnection}, name ${channel.getConnection.getClientProvidedName})")
      }

      override def handleConnectionRecoveryException(conn: Connection, exception: Throwable): Unit = async {
        logger.plainDebug(exception)(s"Recovery error on connection $conn (name ${conn.getClientProvidedName})") >>
          connectionListener.onRecoveryFailure(conn, exception)
      }

      override def handleBlockedListenerException(conn: Connection, exception: Throwable): Unit = async {
        logger.plainInfo(exception)(s"Recovery error on connection $conn (name ${conn.getClientProvidedName})")
      }

      override def handleChannelRecoveryException(ch: Channel, exception: Throwable): Unit = async {
        logger.plainDebug(exception)(s"Recovery error on channel $ch") >>
          channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable): Unit = async {
        exception match {
          case ioe: IOException => logger.plainInfo(ioe)(s"RabbitMQ IO exception on $conn (name ${conn.getClientProvidedName})")
          case e => logger.plainDebug(e)(s"RabbitMQ unexpected exception on $conn (name ${conn.getClientProvidedName})")
        }
      }

      override def handleConsumerException(channel: Channel,
                                           exception: Throwable,
                                           consumer: Consumer,
                                           consumerTag: String,
                                           methodName: String): Unit = async {
        logger.plainDebug(s"Consumer exception on channel $channel, consumer with tag '$consumerTag', method '$methodName'") >> {
          val consumerName = consumer match {
            case c: DefaultRabbitMQConsumer[_, _] => c.base.consumerName
            case _ => "unknown"
          }

          consumerListener.onError(consumer, consumerName, channel, exception)
        }
      }

      override def handleTopologyRecoveryException(conn: Connection, ch: Channel, exception: TopologyRecoveryException): Unit = async {
        logger.plainDebug(exception)(s"Topology recovery error on channel $ch (on connection $conn, name ${conn.getClientProvidedName})") >>
          channelListener.onRecoveryFailure(ch, exception)
      }

      override def handleConfirmListenerException(channel: Channel, exception: Throwable): Unit = async {
        logger.plainDebug(exception)(s"Confirm listener error on channel $channel")
      }

      // recovery listener

      override def handleRecovery(recoverable: Recoverable): Unit = async {
        logger.plainDebug(s"Recovery completed on $recoverable") >> {
          recoverable match {
            case ch: ServerChannel => channelListener.onRecoveryCompleted(ch)
            case conn: ServerConnection => connectionListener.onRecoveryCompleted(conn)
          }
        }
      }

      override def handleRecoveryStarted(recoverable: Recoverable): Unit = async {
        logger.plainDebug(s"Recovery started on $recoverable") >> {
          recoverable match {
            case ch: ServerChannel => channelListener.onRecoveryStarted(ch)
            case conn: ServerConnection => connectionListener.onRecoveryStarted(conn)
          }
        }
      }
    }
}

private[rabbitmq] case class RabbitMQConnectionInfo(hosts: immutable.Seq[String], virtualHost: String, username: Option[String])
