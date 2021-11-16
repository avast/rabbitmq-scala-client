package com.avast.clients.rabbitmq

import cats.effect._
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.{Meter, Monitor}
import com.rabbitmq.client.AMQP.Queue
import com.rabbitmq.client.{AMQP, Consumer}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.event.Level

import java.util.concurrent.TimeoutException
import scala.collection.immutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  private type DefaultDeliveryReadAction[F[_]] = DeliveryReadActionWithMeta[F, Bytes]

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
        consumerListener: ConsumerListener,
        readAction: DeliveryReadAction[F, A],
        middlewares: List[RabbitMQConsumerMiddleware[F, A]],
        blocker: Blocker,
        monitor: Monitor
    )(implicit timer: Timer[F], cs: ContextShift[F]): DefaultRabbitMQConsumer[F] = {

      prepareConsumer(consumerConfig,
                      connectionInfo,
                      republishStrategy,
                      channel,
                      consumerListener,
                      readAction,
                      middlewares,
                      blocker,
                      monitor)
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
                                                             consumerListener: ConsumerListener,
                                                             middlewares: List[RabbitMQStreamingConsumerMiddleware[F, A]])(
        implicit timer: Timer[F],
        cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {

      prepareStreamingConsumer(consumerConfig,
                               connectionInfo,
                               republishStrategy,
                               channel,
                               newChannel,
                               consumerListener,
                               middlewares,
                               blocker,
                               monitor)
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
      middlewares: List[RabbitMQStreamingConsumerMiddleware[F, A]],
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

    val timeoutAction = (d: Delivery[Bytes], mid: MessageId, cid: CorrelationId, consumerLogger: Logger) =>
      doTimeoutAction(name, consumerLogger, d, mid, cid, processTimeout, consumerConfig.timeoutAction, timeoutLogLevel, timeoutsMeter)

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
      middlewares,
      blocker
    )
  }

  private def prepareConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      consumerListener: ConsumerListener,
      readAction: DeliveryReadAction[F, A],
      middlewares: List[RabbitMQConsumerMiddleware[F, A]],
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

    prepareConsumer(consumerConfig, connectionInfo, republishStrategy, channel, readAction, consumerListener, middlewares, blocker, monitor)
  }

  private def prepareConsumer[F[_], A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      channel: ServerChannel,
      userReadAction: DeliveryReadAction[F, A],
      consumerListener: ConsumerListener,
      middlewares: List[RabbitMQConsumerMiddleware[F, A]],
      blocker: Blocker,
      monitor: Monitor)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): DefaultRabbitMQConsumer[F] = {
    import consumerConfig._

    val finalMiddleware = new RabbitMQConsumerMiddleware[F, A] {
      override def adjustDelivery(d: Delivery[A]): F[Delivery[A]] = {
        middlewares.foldLeft(F.pure(d)) { case (prev, m) => prev.flatMap(sd => m.adjustDelivery(sd)) }
      }

      override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = {
        middlewares.foldLeft(r) { case (prev, m) => m.adjustResult(rawDelivery, prev) }
      }
    }

    val readAction = wrapReadAction(consumerConfig, userReadAction, finalMiddleware, monitor, blocker)

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

  private def wrapReadAction[F[_], A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      userReadActionRaw: DeliveryReadAction[F, A],
      finalMiddleware: RabbitMQConsumerMiddleware[F, A],
      consumerMonitor: Monitor,
      blocker: Blocker)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): DefaultDeliveryReadAction[F] = {
    import consumerConfig._

    val fatalFailuresMeter = consumerMonitor.meter("fatalFailures")

    (rawDelivery: Delivery[Bytes], messageId: MessageId, correlationId: CorrelationId, consumerLogger: Logger) =>
      import rawDelivery.routingKey

      val delivery = convertDelivery(rawDelivery)

      val result = try {
        (for {
          adjustedDelivery <- finalMiddleware.adjustDelivery(delivery)
          action = blocker.delay { userReadActionRaw(adjustedDelivery) }.flatten // we try to catch also long-lasting synchronous work on the thread
          timeoutAction = createTimeoutAction(consumerConfig, consumerMonitor, action)
          result <- timeoutAction(rawDelivery, messageId, correlationId, consumerLogger)
        } yield {
          result
        }).recoverWith {
          case NonFatal(e) =>
            fatalFailuresMeter.mark()
            consumerLogger.warn(
              s"[$name] Error while executing callback for delivery with routing key $routingKey, applying DeliveryResult.$failureAction. Delivery was:\n$rawDelivery",
              e
            )
            ConcurrentEffect[F].pure(consumerConfig.failureAction)
        }
      } catch {
        case NonFatal(e) =>
          fatalFailuresMeter.mark()
          consumerLogger.error(
            s"[$name] Error while executing callback for delivery with routing key $routingKey, applying DeliveryResult.$failureAction. Delivery was:\n$rawDelivery",
            e
          )
          ConcurrentEffect[F].pure(consumerConfig.failureAction)
      }

      finalMiddleware.adjustResult(delivery, result)
  }

  private def createTimeoutAction[F[_]: ConcurrentEffect: Timer, A](consumerConfig: ConsumerConfig,
                                                                    consumerMonitor: Monitor,
                                                                    action: F[DeliveryResult]): DefaultDeliveryReadAction[F] = {
    (delivery: Delivery[Bytes], messageId: MessageId, correlationId: CorrelationId, consumerLogger: Logger) =>
      import consumerConfig._

      val timeoutsMeter = consumerMonitor.meter("timeouts")

      if (processTimeout != Duration.Zero) {
        Concurrent
          .timeout(action, processTimeout)
          .recoverWith {
            case e: TimeoutException =>
              consumerLogger.trace(s"[$name] Timeout for $messageId/$correlationId", e)
              doTimeoutAction(name,
                              consumerLogger,
                              delivery,
                              messageId,
                              correlationId,
                              processTimeout,
                              timeoutAction,
                              timeoutLogLevel,
                              timeoutsMeter)
          }
      } else action
  }

  private def doTimeoutAction[F[_]: ConcurrentEffect, A](consumerName: String,
                                                         consumerLogger: Logger,
                                                         delivery: Delivery[Bytes],
                                                         messageId: MessageId,
                                                         correlationId: CorrelationId,
                                                         timeoutLength: FiniteDuration,
                                                         timeoutAction: DeliveryResult,
                                                         timeoutLogLevel: Level,
                                                         timeoutsMeter: Meter): F[DeliveryResult] = Sync[F].delay {

    timeoutsMeter.mark()

    lazy val msg =
      s"[$consumerName] Task timed-out after $timeoutLength of processing delivery $messageId/$correlationId with routing key ${delivery.routingKey}, applying DeliveryResult.$timeoutAction. Delivery was:\n$delivery"

    timeoutLogLevel match {
      case Level.ERROR => consumerLogger.error(msg)
      case Level.WARN => consumerLogger.warn(msg)
      case Level.INFO => consumerLogger.info(msg)
      case Level.DEBUG => consumerLogger.debug(msg)
      case Level.TRACE => consumerLogger.trace(msg)
    }

    timeoutAction
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.view.mapValues(_.asInstanceOf[Object]).toMap.asJava
  }

}
