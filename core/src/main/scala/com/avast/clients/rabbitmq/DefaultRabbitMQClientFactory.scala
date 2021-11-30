package com.avast.clients.rabbitmq

import cats.effect._
import cats.syntax.all._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.Queue
import com.rabbitmq.client.{AMQP, Consumer}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.event.Level

import java.util.concurrent.TimeoutException
import scala.collection.immutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  object Producer {

    def create[F[_]: ConcurrentEffect, A: ProductConverter](
        producerConfig: ProducerConfig,
        connection: RabbitMQConnection[F],
        connectionInfo: RabbitMQConnectionInfo,
        blocker: Blocker,
        monitor: Monitor)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQProducer[F, A]] = {
      prepareProducer[F, A](producerConfig, connection, connectionInfo, blocker, monitor)
    }
  }

  object Consumer {

    def create[F[_]: ConcurrentEffect: Timer: ContextShift, A: DeliveryConverter](
        consumerConfig: ConsumerConfig,
        connection: RabbitMQConnection[F],
        connectionInfo: RabbitMQConnectionInfo,
        republishStrategy: RepublishStrategyConfig,
        consumerListener: ConsumerListener,
        readAction: DeliveryReadAction[F, A],
        blocker: Blocker,
        monitor: Monitor
    ): Resource[F, DefaultRabbitMQConsumer[F, A]] = {
      prepareConsumer[F, A](consumerConfig, connection, connectionInfo, republishStrategy, consumerListener, readAction, blocker, monitor)
    }
  }

  object PullConsumer {

    def create[F[_]: ConcurrentEffect, A: DeliveryConverter](
        consumerConfig: PullConsumerConfig,
        connection: RabbitMQConnection[F],
        connectionInfo: RabbitMQConnectionInfo,
        republishStrategy: RepublishStrategyConfig,
        blocker: Blocker,
        monitor: Monitor)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQPullConsumer[F, A]] = {

      preparePullConsumer(consumerConfig, connection, connectionInfo, republishStrategy, blocker, monitor)
    }
  }

  object StreamingConsumer {

    def create[F[_]: ConcurrentEffect, A: DeliveryConverter](consumerConfig: StreamingConsumerConfig,
                                                             connection: RabbitMQConnection[F],
                                                             connectionInfo: RabbitMQConnectionInfo,
                                                             republishStrategy: RepublishStrategyConfig,
                                                             blocker: Blocker,
                                                             monitor: Monitor,
                                                             consumerListener: ConsumerListener)(
        implicit timer: Timer[F],
        cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {

      prepareStreamingConsumer(consumerConfig, connection, connectionInfo, republishStrategy, consumerListener, blocker, monitor)
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

    def declareQueue[F[_]: Sync](config: DeclareQueueConfig, channel: ServerChannel): F[Unit] =
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
      connection: RabbitMQConnection[F],
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      consumerListener: ConsumerListener,
      blocker: Blocker,
      monitor: Monitor)(implicit timer: Timer[F], cs: ContextShift[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    import consumerConfig._

    connection.newChannel().flatMap { channel =>
      // auto declare exchanges
      declareExchangesFromBindings(connectionInfo, channel, consumerConfig.bindings)

      // auto declare queue; if configured
      consumerConfig.declare.foreach {
        declareQueue(consumerConfig.queueName, connectionInfo, channel, _)
      }

      // auto bind
      bindQueue(connectionInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

      bindQueueForRepublishing(connectionInfo, channel, consumerConfig.queueName, republishStrategy)

      PoisonedMessageHandler.make[F, A](connection, consumerConfig.poisonedMessageHandling).flatMap { pmh =>
        DefaultRabbitMQStreamingConsumer.make(
          name,
          connection.newChannel().evalTap(ch => Sync[F].delay(ch.basicQos(consumerConfig.prefetchCount))),
          consumerTag,
          queueName,
          connectionInfo,
          consumerListener,
          queueBufferSize,
          monitor,
          republishStrategy.toRepublishStrategy,
          processTimeout,
          timeoutAction,
          timeoutLogLevel,
          pmh,
          blocker
        )
      }
    }
  }

  private def prepareConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      connection: RabbitMQConnection[F],
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      consumerListener: ConsumerListener,
      readAction: DeliveryReadAction[F, A],
      blocker: Blocker,
      monitor: Monitor)(implicit timer: Timer[F], cs: ContextShift[F]): Resource[F, DefaultRabbitMQConsumer[F, A]] = {
    connection.newChannel().flatMap { channel =>
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

      createConsumer(consumerConfig, connection, connectionInfo, republishStrategy, channel, readAction, consumerListener, blocker, monitor)
    }
  }

  private def createConsumer[F[_], A: DeliveryConverter](consumerConfig: ConsumerConfig,
                                                         connection: RabbitMQConnection[F],
                                                         connectionInfo: RabbitMQConnectionInfo,
                                                         republishStrategy: RepublishStrategyConfig,
                                                         channel: ServerChannel,
                                                         readAction: DeliveryReadAction[F, A],
                                                         consumerListener: ConsumerListener,
                                                         blocker: Blocker,
                                                         monitor: Monitor)(
      implicit F: ConcurrentEffect[F],
      timer: Timer[F],
      cs: ContextShift[F]): Resource[F, DefaultRabbitMQConsumer[F, A]] = {
    import consumerConfig._

    PoisonedMessageHandler.make[F, A](connection, consumerConfig.poisonedMessageHandling).map { pmh =>
      val consumer = new DefaultRabbitMQConsumer[F, A](
        name,
        channel,
        queueName,
        connectionInfo,
        republishStrategy.toRepublishStrategy,
        pmh,
        processTimeout,
        timeoutAction,
        timeoutLogLevel,
        failureAction,
        consumerListener,
        monitor,
        blocker
      )(readAction)

      startConsumingQueue(channel, queueName, consumerTag, consumer)

      consumer
    }
  }

  private def preparePullConsumer[F[_]: ConcurrentEffect, A: DeliveryConverter](
      consumerConfig: PullConsumerConfig,
      connection: RabbitMQConnection[F],
      connectionInfo: RabbitMQConnectionInfo,
      republishStrategy: RepublishStrategyConfig,
      blocker: Blocker,
      monitor: Monitor)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQPullConsumer[F, A]] = {
    connection.newChannel().flatMap { channel =>
      PoisonedMessageHandler.make[F, A](connection, consumerConfig.poisonedMessageHandling).map { pmh =>
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

        new DefaultRabbitMQPullConsumer[F, A](
          name,
          channel,
          queueName,
          connectionInfo,
          republishStrategy.toRepublishStrategy,
          pmh,
          monitor,
          blocker
        )
      }
    }
  }

  private def prepareProducer[F[_]: ConcurrentEffect, A: ProductConverter](
      producerConfig: ProducerConfig,
      connection: RabbitMQConnection[F],
      connectionInfo: RabbitMQConnectionInfo,
      blocker: Blocker,
      monitor: Monitor)(implicit cs: ContextShift[F]): Resource[F, DefaultRabbitMQProducer[F, A]] = {
    connection.newChannel().map { channel =>
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

  private[rabbitmq] def convertDelivery[F[_], A: DeliveryConverter](d: Delivery[Bytes]): Delivery[A] = {
    d.flatMap { d =>
      implicitly[DeliveryConverter[A]].convert(d.body) match {
        case Right(a) => d.mapBody(_ => a)
        case Left(ce) => Delivery.MalformedContent(d.body, d.properties, d.routingKey, ce)
      }
    }
  }

  private[rabbitmq] def watchForTimeoutIfConfigured[F[_]: ConcurrentEffect: Timer, A](
      consumerName: String,
      consumerLogger: Logger,
      consumerMonitor: Monitor,
      processTimeout: FiniteDuration,
      timeoutAction: DeliveryResult,
      timeoutLogLevel: Level)(delivery: Delivery[A], messageId: MessageId, correlationId: CorrelationId, result: F[DeliveryResult])(
      customTimeoutAction: F[Unit],
  ): F[DeliveryResult] = {

    val timeoutsMeter = consumerMonitor.meter("timeouts")

    if (processTimeout != Duration.Zero) {
      Concurrent
        .timeout(result, processTimeout)
        .recoverWith {
          case e: TimeoutException =>
            customTimeoutAction >>
              Sync[F].delay {
                consumerLogger.trace(s"[$consumerName] Timeout for $messageId/$correlationId", e)

                timeoutsMeter.mark()

                lazy val msg =
                  s"[$consumerName] Task timed-out after $processTimeout of processing delivery $messageId/$correlationId with routing key ${delivery.routingKey}, applying DeliveryResult.$timeoutAction. Delivery was:\n$delivery"

                timeoutLogLevel match {
                  case Level.ERROR => consumerLogger.error(msg)
                  case Level.WARN => consumerLogger.warn(msg)
                  case Level.INFO => consumerLogger.info(msg)
                  case Level.DEBUG => consumerLogger.debug(msg)
                  case Level.TRACE => consumerLogger.trace(msg)
                }

                timeoutAction
              }
        }
    } else result
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.view.mapValues(_.asInstanceOf[Object]).toMap.asJava
  }

}
