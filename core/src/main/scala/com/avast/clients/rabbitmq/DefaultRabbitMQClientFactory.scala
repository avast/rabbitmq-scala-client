package com.avast.clients.rabbitmq

import java.util.concurrent.TimeoutException

import cats.effect._
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.{Meter, Monitor}
import com.rabbitmq.client.AMQP.Queue
import com.rabbitmq.client.{AMQP, Consumer}
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.event.Level

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.language.{higherKinds, implicitConversions}
import scala.util.control.NonFatal

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  private type DefaultDeliveryReadAction[F[_]] = DeliveryReadAction[F, Bytes]

  object Producer {

    def create[F[_]: ConcurrentEffect, A: ProductConverter](
        producerConfig: ProducerConfig,
        channel: ServerChannel,
        factoryInfo: RabbitMQConnectionInfo,
        blocker: Blocker,
        monitor: Monitor)(implicit cs: ContextShift[F]): DefaultRabbitMQProducer[F, A] = {
      prepareProducer[F, A](producerConfig, channel, factoryInfo, blocker, monitor)
    }

  }

  object Consumer {

    def create[F[_]: ConcurrentEffect, A: DeliveryConverter](
        consumerConfig: ConsumerConfig,
        channel: ServerChannel,
        connectionInfo: RabbitMQConnectionInfo,
        republishStrategy: RepublishStrategyConfig,
        blocker: Blocker,
        monitor: Monitor,
        consumerListener: ConsumerListener,
        readAction: DeliveryReadAction[F, A])(implicit timer: Timer[F], cs: ContextShift[F]): DefaultRabbitMQConsumer[F] = {

      prepareConsumer(consumerConfig, readAction, connectionInfo, republishStrategy, channel, consumerListener, blocker, monitor)
    }
  }

  object PullConsumer {

    def create[F[_]: ConcurrentEffect, A: DeliveryConverter](
        consumerConfig: PullConsumerConfig,
        channel: ServerChannel,
        connectionInfo: RabbitMQConnectionInfo,
        republishStrategy: RepublishStrategyConfig,
        blocker: Blocker,
        monitor: Monitor)(implicit cs: ContextShift[F]): DefaultRabbitMQPullConsumer[F, A] = {

      preparePullConsumer(consumerConfig, connectionInfo, republishStrategy, channel, blocker, monitor)
    }
  }

  object StreamingConsumer {

    def create[F[_]: ConcurrentEffect, A: DeliveryConverter](consumerConfig: StreamingConsumerConfig,
                                                             channel: ServerChannel,
                                                             newChannel: F[ServerChannel],
                                                             connectionInfo: RabbitMQConnectionInfo,
                                                             republishStrategy: RepublishStrategyConfig,
                                                             blocker: Blocker,
                                                             monitor: Monitor,
                                                             consumerListener: ConsumerListener)(
        implicit timer: Timer[F],
        cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {

      prepareStreamingConsumer(consumerConfig, connectionInfo, republishStrategy, channel, newChannel, consumerListener, blocker, monitor)
    }
  }

  object Declarations {
    def declareExchange[F[_]: Sync](config: DeclareExchangeConfig,
                                    channel: ServerChannel,
                                    connectionInfo: RabbitMQConnectionInfo): F[Unit] =
      Sync[F].delay {
        import config._

        DefaultRabbitMQClientFactory.this.declareExchange(name, `type`, durable, autoDelete, arguments, channel, connectionInfo)
      }

    def declareQueue[F[_]: Sync](config: DeclareQueueConfig, channel: ServerChannel, connectionInfo: RabbitMQConnectionInfo): F[Unit] =
      Sync[F].delay {
        import config._

        DefaultRabbitMQClientFactory.this.declareQueue(channel, name, durable, exclusive, autoDelete, arguments)
        ()
      }

    def bindQueue[F[_]: Sync](config: BindQueueConfig, channel: ServerChannel, connectionInfo: RabbitMQConnectionInfo): F[Unit] =
      Sync[F].delay {
        import config._

        DefaultRabbitMQClientFactory.bindQueue(channel, queueName, exchangeName, routingKeys, arguments, connectionInfo)
      }

    def bindExchange[F[_]: Sync](config: BindExchangeConfig, channel: ServerChannel, connectionInfo: RabbitMQConnectionInfo): F[Unit] =
      Sync[F].delay {
        import config._

        routingKeys.foreach {
          DefaultRabbitMQClientFactory.this
            .bindExchange(connectionInfo)(channel, sourceExchangeName, destExchangeName, arguments.value)
        }
      }
  }

  private def prepareStreamingConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: StreamingConsumerConfig,
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      newChannel: F[ServerChannel],
      consumerListener: ConsumerListener,
      blocker: Blocker,
      monitor: Monitor)(implicit timer: Timer[F], cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    import consumerConfig._

    val timeoutsMeter = monitor.meter("timeouts")

    // auto declare exchanges
    declareExchangesFromBindings(connectionInfo, channel, consumerConfig.bindings)

    // auto declare queue; if configured
    consumerConfig.declare.foreach {
      declareQueue(consumerConfig.queueName, connectionInfo, channel, _)
    }

    // auto bind
    bindQueue(connectionInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    bindQueueForRepublishing(connectionInfo, channel, consumerConfig.queueName, republishStrategy)

    val timeoutAction = (e: TimeoutException) => doTimeoutAction(name, consumerConfig.timeoutAction, timeoutLogLevel, timeoutsMeter, e)

    DefaultRabbitMQStreamingConsumer.make(
      name,
      newChannel.flatTap(ch => Sync[F].delay(ch.basicQos(consumerConfig.prefetchCount))),
      consumerTag,
      queueName,
      connectionInfo,
      consumerListener,
      queueBufferSize,
      monitor,
      republishStrategy.toRepublishStrategy,
      processTimeout,
      timeoutAction,
      blocker
    )
  }

  private def prepareConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      readAction: DeliveryReadAction[F, A],
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      consumerListener: ConsumerListener,
      blocker: Blocker,
      monitor: Monitor)(implicit timer: Timer[F], cs: ContextShift[F]): DefaultRabbitMQConsumer[F] = {

    // auto declare exchanges
    declareExchangesFromBindings(connectionInfo, channel, consumerConfig.bindings)

    // auto declare queue; if configured
    consumerConfig.declare.foreach {
      declareQueue(consumerConfig.queueName, connectionInfo, channel, _)
    }

    // set prefetch size (per consumer)
    channel.basicQos(consumerConfig.prefetchCount)

    // auto bind
    bindQueue(connectionInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    bindQueueForRepublishing(connectionInfo, channel, consumerConfig.queueName, republishStrategy)

    prepareConsumer(consumerConfig, connectionInfo, republishStrategy, channel, readAction, consumerListener, blocker, monitor)
  }

  private def prepareConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      userReadAction: DeliveryReadAction[F, A],
      consumerListener: ConsumerListener,
      blocker: Blocker,
      monitor: Monitor)(implicit timer: Timer[F], cs: ContextShift[F]): DefaultRabbitMQConsumer[F] = {
    import consumerConfig._

    val readAction: DefaultDeliveryReadAction[F] = {
      val convAction: DefaultDeliveryReadAction[F] = { d: Delivery[Bytes] =>
        try {
          userReadAction(convertDelivery(d))
        } catch {
          case NonFatal(e) =>
            ConcurrentEffect[F].raiseError(e)
        }
      }

      wrapReadAction(consumerConfig, convAction, monitor, blocker)
    }

    val consumer = {
      new DefaultRabbitMQConsumer(name,
                                  channel,
                                  queueName,
                                  connectionInfo,
                                  monitor,
                                  failureAction,
                                  consumerListener,
                                  republishStrategy.toRepublishStrategy,
                                  blocker)(readAction)
    }

    startConsumingQueue(channel, queueName, consumerTag, consumer)

    consumer
  }

  private def preparePullConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: PullConsumerConfig,
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      blocker: Blocker,
      monitor: Monitor)(implicit cs: ContextShift[F]): DefaultRabbitMQPullConsumer[F, A] = {

    import consumerConfig._

    // auto declare exchanges
    declareExchangesFromBindings(connectionInfo, channel, consumerConfig.bindings)

    // auto declare queue; if configured
    declare.foreach {
      declareQueue(consumerConfig.queueName, connectionInfo, channel, _)
    }

    // auto bind
    bindQueue(connectionInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    bindQueueForRepublishing(connectionInfo, channel, consumerConfig.queueName, republishStrategy)

    new DefaultRabbitMQPullConsumer[F, A](name,
                                          channel,
                                          queueName,
                                          connectionInfo,
                                          failureAction,
                                          monitor,
                                          republishStrategy.toRepublishStrategy,
                                          blocker)
  }

  private def prepareProducer[F[_]: ConcurrentEffect, A: ProductConverter](
      producerConfig: ProducerConfig,
      channel: ServerChannel,
      connectionInfo: RabbitMQConnectionInfo,
      blocker: Blocker,
      monitor: Monitor)(implicit cs: ContextShift[F]): DefaultRabbitMQProducer[F, A] = {

    val defaultProperties = MessageProperties(
      deliveryMode = DeliveryMode.fromCode(producerConfig.properties.deliveryMode),
      contentType = producerConfig.properties.contentType,
      contentEncoding = producerConfig.properties.contentEncoding,
      priority = producerConfig.properties.priority.map(Integer.valueOf)
    )

    // auto declare exchange; if configured
    producerConfig.declare.foreach {
      declareExchange(producerConfig.exchange, connectionInfo, channel, _)
    }

    new DefaultRabbitMQProducer[F, A](
      producerConfig.name,
      producerConfig.exchange,
      channel,
      defaultProperties,
      producerConfig.reportUnroutable,
      blocker,
      monitor
    )
  }

  private[rabbitmq] def declareExchange(name: String,
                                        connectionInfo: RabbitMQConnectionInfo,
                                        channel: ServerChannel,
                                        autoDeclareExchange: AutoDeclareExchangeConfig): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      declareExchange(name, `type`, durable, autoDelete, arguments, channel, connectionInfo)
    }
    ()
  }

  private[rabbitmq] def declareExchange(name: String,
                                        `type`: ExchangeType,
                                        durable: Boolean,
                                        autoDelete: Boolean,
                                        arguments: DeclareArgumentsConfig,
                                        channel: ServerChannel,
                                        connectionInfo: RabbitMQConnectionInfo): Unit = {
    logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${connectionInfo.virtualHost}'")
    val javaArguments = argsAsJava(arguments.value)
    channel.exchangeDeclare(name, `type`.value, durable, autoDelete, javaArguments)
    ()
  }

  private def bindQueue(connectionInfo: RabbitMQConnectionInfo,
                        channel: ServerChannel,
                        queueName: String,
                        bindings: immutable.Seq[AutoBindQueueConfig]): Unit = {
    bindings.foreach { bind =>
      import bind._
      val exchangeName = bind.exchange.name

      bindQueue(channel, queueName, exchangeName, routingKeys, bindArguments, connectionInfo)
    }
  }

  private def bindQueue(channel: ServerChannel,
                        queueName: String,
                        exchangeName: String,
                        routingKeys: immutable.Seq[String],
                        bindArguments: BindArgumentsConfig,
                        connectionInfo: RabbitMQConnectionInfo): Unit = {
    if (routingKeys.nonEmpty) {
      routingKeys.foreach { routingKey =>
        bindQueue(connectionInfo)(channel, queueName)(exchangeName, routingKey, bindArguments.value)
      }
    } else {
      // binding without routing key, possibly to fanout exchange

      bindQueue(connectionInfo)(channel, queueName)(exchangeName, "", bindArguments.value)
    }
  }

  private[rabbitmq] def startConsumingQueue(channel: ServerChannel, queueName: String, consumerTag: String, consumer: Consumer): String = {
    channel.setDefaultConsumer(consumer) // see `setDefaultConsumer` javadoc; this is possible because the channel is here exclusively for this consumer
    val finalConsumerTag = channel.basicConsume(queueName, false, if (consumerTag == "Default") "" else consumerTag, consumer)
    finalConsumerTag
  }

  private def bindQueueForRepublishing[F[_]](connectionInfo: RabbitMQConnectionInfo,
                                             channel: ServerChannel,
                                             queueName: String,
                                             strategyConfig: RepublishStrategyConfig): Unit = {
    import RepublishStrategyConfig._

    strategyConfig match {
      case CustomExchange(exchangeName, _, exchangeAutoBind) =>
        if (exchangeAutoBind) {
          bindQueue(connectionInfo)(channel, queueName)(exchangeName, queueName, Map.empty)
        }

      case _ => () // no-op
    }
  }

  private def declareQueue(queueName: String,
                           connectionInfo: RabbitMQConnectionInfo,
                           channel: ServerChannel,
                           declare: AutoDeclareQueueConfig): Unit = {
    import declare._

    if (enabled) {
      logger.info(s"Declaring queue '$queueName' in virtual host '${connectionInfo.virtualHost}'")
      declareQueue(channel, queueName, durable, exclusive, autoDelete, arguments)
    }
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean,
                                     arguments: DeclareArgumentsConfig): Queue.DeclareOk = {
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.value)
  }

  private[rabbitmq] def bindQueue(connectionInfo: RabbitMQConnectionInfo)(
      channel: ServerChannel,
      queueName: String)(exchangeName: String, routingKey: String, arguments: ArgumentsMap): AMQP.Queue.BindOk = {
    logger.info(s"Binding exchange $exchangeName($routingKey) -> queue '$queueName' in virtual host '${connectionInfo.virtualHost}'")

    channel.queueBind(queueName, exchangeName, routingKey, arguments)
  }

  private[rabbitmq] def bindExchange(connectionInfo: RabbitMQConnectionInfo)(
      channel: ServerChannel,
      sourceExchangeName: String,
      destExchangeName: String,
      arguments: ArgumentsMap)(routingKey: String): AMQP.Exchange.BindOk = {
    logger.info(
      s"Binding exchange $sourceExchangeName($routingKey) -> exchange '$destExchangeName' in virtual host '${connectionInfo.virtualHost}'"
    )

    channel.exchangeBind(destExchangeName, sourceExchangeName, routingKey, arguments)
  }

  private def declareExchangesFromBindings(connectionInfo: RabbitMQConnectionInfo,
                                           channel: ServerChannel,
                                           bindings: Seq[AutoBindQueueConfig]): Unit = {
    bindings.foreach { bind =>
      import bind.exchange._

      // auto declare exchange; if configured
      declare.foreach {
        declareExchange(name, connectionInfo, channel, _)
      }
    }
  }

  private[rabbitmq] def convertDelivery[F[_]: ConcurrentEffect, A: DeliveryConverter](d: Delivery[Bytes]): Delivery[A] = {
    d.flatMap { d =>
      implicitly[DeliveryConverter[A]].convert(d.body) match {
        case Right(a) => d.mapBody(_ => a)
        case Left(ce) => Delivery.MalformedContent(d.body, d.properties, d.routingKey, ce)
      }
    }
  }

  private def wrapReadAction[F[_]: ConcurrentEffect, A](
      consumerConfig: ConsumerConfig,
      userReadAction: DefaultDeliveryReadAction[F],
      consumerMonitor: Monitor,
      blocker: Blocker)(implicit timer: Timer[F], cs: ContextShift[F]): DefaultDeliveryReadAction[F] = {
    import consumerConfig._

    val timeoutsMeter = consumerMonitor.meter("timeouts")
    val fatalFailuresMeter = consumerMonitor.meter("fatalFailures")

    delivery: Delivery[Bytes] =>
      try {
        // we try to catch also long-lasting synchronous work on the thread
        val action: F[DeliveryResult] = blocker.delay { userReadAction(delivery) }.flatten

        val timedOutAction: F[DeliveryResult] = {
          if (processTimeout != Duration.Zero) {
            Concurrent
              .timeout(action, processTimeout)
              .recoverWith {
                case e: TimeoutException => doTimeoutAction(name, timeoutAction, timeoutLogLevel, timeoutsMeter, e)
              }
          } else action
        }

        timedOutAction
          .recoverWith {
            case NonFatal(e) =>
              fatalFailuresMeter.mark()
              logger.warn(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
              ConcurrentEffect[F].pure(consumerConfig.failureAction)
          }
      } catch {
        case NonFatal(e) =>
          fatalFailuresMeter.mark()
          logger.error(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
          ConcurrentEffect[F].pure(consumerConfig.failureAction)
      }
  }

  private def doTimeoutAction[A, F[_]: ConcurrentEffect](consumerName: String,
                                                         timeoutAction: DeliveryResult,
                                                         timeoutLogLevel: Level,
                                                         timeoutsMeter: Meter,
                                                         e: TimeoutException): F[DeliveryResult] = Sync[F].delay {

    timeoutsMeter.mark()

    lazy val msg = s"[$consumerName] Task timed-out, applying DeliveryResult.$timeoutAction"

    timeoutLogLevel match {
      case Level.ERROR => logger.error(msg, e)
      case Level.WARN => logger.warn(msg, e)
      case Level.INFO => logger.info(msg, e)
      case Level.DEBUG => logger.debug(msg, e)
      case Level.TRACE => logger.trace(msg, e)
    }

    timeoutAction
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.mapValues(_.asInstanceOf[Object]).asJava
  }

}
