package com.avast.clients.rabbitmq

import java.time.Duration
import java.util.concurrent.{TimeUnit, TimeoutException}

import cats.effect.Effect
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Reject, Republish, Retry}
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.{Meter, Monitor}
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.Queue
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader
import org.slf4j.event.Level

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.language.{higherKinds, implicitConversions}
import scala.util.control.NonFatal

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  private type DefaultDeliveryReadAction[F[_]] = DeliveryReadAction[F, Bytes]

  private[rabbitmq] val FakeConfigRootName = "_rabbitmq_config_root_"

  private[rabbitmq] final val DeclareQueueRootConfigKey = "avastRabbitMQDeclareQueueDefaults"
  private[rabbitmq] final val DeclareQueueDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareQueueRootConfigKey)

  private[rabbitmq] final val BindQueueRootConfigKey = "avastRabbitMQBindQueueDefaults"
  private[rabbitmq] final val BindQueueDefaultConfig = ConfigFactory.defaultReference().getConfig(BindQueueRootConfigKey)

  private[rabbitmq] final val BindExchangeRootConfigKey = "avastRabbitMQBindExchangeDefaults"
  private[rabbitmq] final val BindExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(BindExchangeRootConfigKey)

  private[rabbitmq] final val DeclareExchangeRootConfigKey = "avastRabbitMQDeclareExchangeDefaults"
  private[rabbitmq] final val DeclareExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareExchangeRootConfigKey)

  private[rabbitmq] final val ProducerRootConfigKey = "avastRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareExchangeDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerRootConfigKey = "avastRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareQueueDefaultConfig).root())
  }
  private[rabbitmq] final val PullConsumerRootConfigKey = "avastRabbitMQPullConsumerDefaults"
  private[rabbitmq] final val PullConsumerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareQueueDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerBindingRootConfigKey = "avastRabbitMQConsumerBindingDefaults"
  private[rabbitmq] final val ConsumerBindingDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerBindingRootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = (config: Config, path: String) => config.getDuration(path)

  private implicit final val DeliveryResultReader: ValueReader[DeliveryResult] = (config: Config, path: String) =>
    config.getString(path).toLowerCase match {
      case "ack" => Ack
      case "reject" => Reject
      case "retry" => Retry
      case "republish" => Republish()
  }

  private implicit final val rabbitDeclareArgumentsReader: ValueReader[DeclareArguments] = (config: Config, path: String) => {
    import scala.collection.JavaConverters._
    val argumentsMap = config
      .getObject(path)
      .asScala
      .toMap
      .mapValues(_.unwrapped())

    DeclareArguments(argumentsMap)
  }

  private implicit final val rabbitBindArgumentsReader: ValueReader[BindArguments] = (config: Config, path: String) => {
    import scala.collection.JavaConverters._
    val argumentsMap = config
      .getObject(path)
      .asScala
      .toMap
      .mapValues(_.unwrapped())

    BindArguments(argumentsMap)
  }

  private implicit final val logLevelReader: ValueReader[Level] = ValueReader[String].map(Level.valueOf)

  // this overrides Ficus's default reader and pass better path into possible exception
  private implicit def traversableReader[C[_], A](implicit entryReader: ValueReader[A],
                                                  cbf: CanBuildFrom[Nothing, A, C[A]]): ValueReader[C[A]] =
    (config: Config, path: String) => {
      val list = config.getList(path).asScala
      val builder = cbf()
      builder.sizeHint(list.size)
      list.zipWithIndex.foreach {
        case (entry, index) =>
          val entryConfig = entry.atPath(s"$path.$index")
          builder += entryReader.read(entryConfig, s"$path.$index")
      }

      builder.result
    }

  object Producer {

    def fromConfig[F[_]: Effect, A: ProductConverter](providedConfig: Config,
                                                      configName: String,
                                                      channel: ServerChannel,
                                                      factoryInfo: RabbitMQConnectionInfo,
                                                      blockingScheduler: Scheduler,
                                                      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQProducer[F, A] = {
      val producerConfig = providedConfig.wrapped(configName).as[ProducerConfig](configName)
      create[F, A](producerConfig, configName, channel, factoryInfo, blockingScheduler, monitor)
    }

    def create[F[_]: Effect, A: ProductConverter](producerConfig: ProducerConfig,
                                                  configName: String,
                                                  channel: ServerChannel,
                                                  factoryInfo: RabbitMQConnectionInfo,
                                                  blockingScheduler: Scheduler,
                                                  monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQProducer[F, A] = {
      prepareProducer[F, A](producerConfig, configName, channel, factoryInfo, blockingScheduler, monitor)
    }

  }

  object Consumer {

    def fromConfig[F[_]: Effect, A: DeliveryConverter](
        providedConfig: Config,
        configName: String,
        channel: ServerChannel,
        channelFactoryInfo: RabbitMQConnectionInfo,
        blockingScheduler: Scheduler,
        monitor: Monitor,
        consumerListener: ConsumerListener,
        readAction: DeliveryReadAction[F, A])(implicit scheduler: Scheduler): DefaultRabbitMQConsumer[F] = {

      val mergedConfig = providedConfig.withFallback(ConsumerDefaultConfig)

      // merge consumer binding defaults
      val updatedConfig = {
        val updated =
          mergedConfig.as[Seq[Config]]("bindings").map { bindConfig =>
            bindConfig.withFallback(ConsumerBindingDefaultConfig).root()
          }

        import scala.collection.JavaConverters._

        mergedConfig.withValue("bindings", ConfigValueFactory.fromIterable(updated.asJava))
      }

      val consumerConfig = updatedConfig.wrapped(configName).as[ConsumerConfig](configName)

      create[F, A](consumerConfig, configName, channel, channelFactoryInfo, blockingScheduler, monitor, consumerListener, readAction)
    }

    def create[F[_]: Effect, A: DeliveryConverter](
        consumerConfig: ConsumerConfig,
        configName: String,
        channel: ServerChannel,
        channelFactoryInfo: RabbitMQConnectionInfo,
        blockingScheduler: Scheduler,
        monitor: Monitor,
        consumerListener: ConsumerListener,
        readAction: DeliveryReadAction[F, A])(implicit scheduler: Scheduler): DefaultRabbitMQConsumer[F] = {

      prepareConsumer(consumerConfig, configName, readAction, channelFactoryInfo, channel, consumerListener, blockingScheduler, monitor)
    }
  }

  object PullConsumer {

    def fromConfig[F[_]: Effect, A: DeliveryConverter](
        providedConfig: Config,
        configName: String,
        channel: ServerChannel,
        channelFactoryInfo: RabbitMQConnectionInfo,
        blockingScheduler: Scheduler,
        monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQPullConsumer[F, A] = {

      val mergedConfig = providedConfig.withFallback(PullConsumerDefaultConfig)

      // merge consumer binding defaults
      val updatedConfig = {
        val updated =
          mergedConfig.as[Seq[Config]]("bindings").map { bindConfig =>
            bindConfig.withFallback(ConsumerBindingDefaultConfig).root()
          }

        import scala.collection.JavaConverters._

        mergedConfig.withValue("bindings", ConfigValueFactory.fromIterable(updated.asJava))
      }

      val consumerConfig = updatedConfig.wrapped(configName).as[PullConsumerConfig](configName)

      create[F, A](consumerConfig, configName, channel, channelFactoryInfo, blockingScheduler, monitor)
    }

    def create[F[_]: Effect, A: DeliveryConverter](consumerConfig: PullConsumerConfig,
                                                   configName: String,
                                                   channel: ServerChannel,
                                                   channelFactoryInfo: RabbitMQConnectionInfo,
                                                   blockingScheduler: Scheduler,
                                                   monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQPullConsumer[F, A] = {

      preparePullConsumer(consumerConfig, configName, channelFactoryInfo, channel, blockingScheduler, monitor)
    }
  }

  object Declarations {
    def declareExchange(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = {
      declareExchange(config.withFallback(DeclareExchangeDefaultConfig).as[DeclareExchange], channel, channelFactoryInfo)
    }

    def declareQueue(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = {
      declareQueue(config.withFallback(DeclareQueueDefaultConfig).as[DeclareQueue], channel, channelFactoryInfo)
    }

    def bindQueue(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = {
      bindQueue(config.withFallback(BindQueueDefaultConfig).as[BindQueue], channel, channelFactoryInfo)
    }

    def bindExchange(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = {
      bindExchange(config.withFallback(BindExchangeDefaultConfig).as[BindExchange], channel, channelFactoryInfo)
    }

    private def declareExchange(config: DeclareExchange, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] =
      Task {
        import config._

        DefaultRabbitMQClientFactory.this.declareExchange(name, `type`, durable, autoDelete, arguments, channel, channelFactoryInfo)
      }

    private def declareQueue(config: DeclareQueue, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = Task {
      import config._

      DefaultRabbitMQClientFactory.this.declareQueue(channel, name, durable, exclusive, autoDelete, arguments)
      ()
    }

    private def bindQueue(config: BindQueue, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = Task {
      import config._

      bindQueues(channel, queueName, exchangeName, routingKeys, arguments, channelFactoryInfo)
    }

    private def bindExchange(config: BindExchange, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = Task {
      import config._

      routingKeys.foreach {
        DefaultRabbitMQClientFactory.this
          .bindExchange(channelFactoryInfo)(channel, sourceExchangeName, destExchangeName, arguments.value)
      }
    }
  }

  private def prepareProducer[F[_]: Effect, A: ProductConverter](
      producerConfig: ProducerConfig,
      configName: String,
      channel: ServerChannel,
      channelFactoryInfo: RabbitMQConnectionInfo,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQProducer[F, A] = {
    import producerConfig._

    val defaultProperties = MessageProperties(
      deliveryMode = DeliveryMode.fromCode(producerConfig.properties.deliveryMode),
      contentType = producerConfig.properties.contentType,
      contentEncoding = producerConfig.properties.contentEncoding,
      priority = producerConfig.properties.priority.map(Integer.valueOf)
    )

    // auto declare of exchange
    // parse it only if it's needed
    // "Lazy" parsing, because exchange type is not part of reference.conf and we don't want to make it fail on missing type when enabled=false
    if (declare.getBoolean("enabled")) {
      val path = s"$configName.declare"
      val d = declare.wrapped(path).as[AutoDeclareExchange](path)

      declareExchange(exchange, channelFactoryInfo, channel, d)
    }
    new DefaultRabbitMQProducer[F, A](producerConfig.name,
                                      exchange,
                                      channel,
                                      defaultProperties,
                                      reportUnroutable,
                                      blockingScheduler,
                                      monitor)
  }

  private[rabbitmq] def declareExchange(name: String,
                                        connectionInfo: RabbitMQConnectionInfo,
                                        channel: ServerChannel,
                                        autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      declareExchange(name, `type`, durable, autoDelete, arguments, channel, connectionInfo)
    }
    ()
  }

  private def declareExchange(name: String,
                              `type`: String,
                              durable: Boolean,
                              autoDelete: Boolean,
                              arguments: DeclareArguments,
                              channel: ServerChannel,
                              connectionInfo: RabbitMQConnectionInfo): Unit = {
    logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${connectionInfo.virtualHost}'")
    val javaArguments = argsAsJava(arguments.value)
    channel.exchangeDeclare(name, `type`, durable, autoDelete, javaArguments)
    ()
  }

  private def prepareConsumer[F[_]: Effect, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      configName: String,
      readAction: DeliveryReadAction[F, A],
      channelFactoryInfo: RabbitMQConnectionInfo,
      channel: ServerChannel,
      consumerListener: ConsumerListener,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQConsumer[F] = {

    // auto declare exchanges
    declareExchangesFromBindings(configName, channelFactoryInfo, channel, consumerConfig.bindings)

    // auto declare queue
    declareQueue(consumerConfig.queueName, channelFactoryInfo, channel, consumerConfig.declare)

    // set prefetch size (per consumer)
    channel.basicQos(consumerConfig.prefetchCount)

    // auto bind
    bindQueues(channelFactoryInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    prepareConsumer(consumerConfig, channelFactoryInfo, channel, readAction, consumerListener, blockingScheduler, monitor)
  }

  private def preparePullConsumer[F[_]: Effect, A: DeliveryConverter](
      consumerConfig: PullConsumerConfig,
      configName: String,
      connectionInfo: RabbitMQConnectionInfo,
      channel: ServerChannel,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQPullConsumer[F, A] = {

    import consumerConfig._

    // auto declare exchanges
    declareExchangesFromBindings(configName, connectionInfo, channel, consumerConfig.bindings)

    // auto declare queue
    declareQueue(queueName, connectionInfo, channel, declare)

    // auto bind
    bindQueues(connectionInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    new DefaultRabbitMQPullConsumer[F, A](name, channel, queueName, connectionInfo, failureAction, monitor, blockingScheduler)
  }

  private def declareQueue(queueName: String,
                           connectionInfo: RabbitMQConnectionInfo,
                           channel: ServerChannel,
                           declare: AutoDeclareQueue): Unit = {
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
                                     arguments: DeclareArguments): Queue.DeclareOk = {
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.value)
  }

  private def bindQueues(connectionInfo: RabbitMQConnectionInfo,
                         channel: ServerChannel,
                         queueName: String,
                         bindings: immutable.Seq[AutoBindQueue]): Unit = {
    bindings.foreach { bind =>
      import bind._
      val exchangeName = bind.exchange.name

      bindQueues(channel, queueName, exchangeName, routingKeys, bindArguments, connectionInfo)
    }
  }

  private def bindQueues(channel: ServerChannel,
                         queueName: String,
                         exchangeName: String,
                         routingKeys: immutable.Seq[String],
                         bindArguments: BindArguments,
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

  private[rabbitmq] def bindQueue(connectionInfo: RabbitMQConnectionInfo)(
      channel: ServerChannel,
      queueName: String)(exchangeName: String, routingKey: String, arguments: ArgumentsMap): AMQP.Queue.BindOk = {
    logger.info(s"Binding exchange $exchangeName($routingKey) -> queue '$queueName' in virtual host '${connectionInfo.virtualHost}'")

    channel.queueBind(queueName, exchangeName, routingKey, arguments)
  }

  private[rabbitmq] def bindExchange(channelFactoryInfo: RabbitMQConnectionInfo)(
      channel: ServerChannel,
      sourceExchangeName: String,
      destExchangeName: String,
      arguments: ArgumentsMap)(routingKey: String): AMQP.Exchange.BindOk = {
    logger.info(
      s"Binding exchange $sourceExchangeName($routingKey) -> exchange '$destExchangeName' in virtual host '${channelFactoryInfo.virtualHost}'"
    )

    channel.exchangeBind(destExchangeName, sourceExchangeName, routingKey, arguments)
  }

  private def declareExchangesFromBindings(configName: String,
                                           connectionInfo: RabbitMQConnectionInfo,
                                           channel: ServerChannel,
                                           bindings: Seq[AutoBindQueue]): Unit = {
    bindings.zipWithIndex.foreach {
      case (bind, i) =>
        import bind.exchange._

        // parse it only if it's needed
        if (declare.getBoolean("enabled")) {
          val path = s"$configName.bindings.$i.exchange.declare"
          val d = declare.wrapped(path).as[AutoDeclareExchange](path)

          declareExchange(name, connectionInfo, channel, d)
        }
    }
  }

  private def prepareConsumer[F[_]: Effect, A: DeliveryConverter](consumerConfig: ConsumerConfig,
                                                                  connectionInfo: RabbitMQConnectionInfo,
                                                                  channel: ServerChannel,
                                                                  userReadAction: DeliveryReadAction[F, A],
                                                                  consumerListener: ConsumerListener,
                                                                  blockingScheduler: Scheduler,
                                                                  monitor: Monitor)(implicit sch: Scheduler): DefaultRabbitMQConsumer[F] = {
    import consumerConfig._

    val readAction: DefaultDeliveryReadAction[F] = {
      val convAction: DefaultDeliveryReadAction[F] = { d: Delivery[Bytes] =>
        try {
          val devA = d.flatMap { d =>
            implicitly[DeliveryConverter[A]].convert(d.body) match {
              case Right(a) => d.mapBody(_ => a)
              case Left(ce) => Delivery.MalformedContent(d.body, d.properties, d.routingKey, ce)
            }
          }

          userReadAction(devA)
        } catch {
          case NonFatal(e) =>
            Effect[F].raiseError(e)
        }
      }

      wrapReadAction(consumerConfig, convAction, monitor, blockingScheduler)
    }

    val consumer = {
      new DefaultRabbitMQConsumer(name, channel, queueName, connectionInfo, monitor, failureAction, consumerListener, blockingScheduler)(readAction)
    }

    val finalConsumerTag = if (consumerTag == "Default") "" else consumerTag

    channel.basicConsume(queueName, false, finalConsumerTag, consumer)
    channel.setDefaultConsumer(consumer) // see `setDefaultConsumer` javadoc; this is possible because the channel is here exclusively for this consumer

    consumer
  }

  private def wrapReadAction[F[_]: Effect, A](consumerConfig: ConsumerConfig,
                                              userReadAction: DefaultDeliveryReadAction[F],
                                              consumerMonitor: Monitor,
                                              blockingScheduler: Scheduler)(implicit sch: Scheduler): DefaultDeliveryReadAction[F] = {
    import consumerConfig._

    val timeoutsMeter = consumerMonitor.meter("timeouts")
    val fatalFailuresMeter = consumerMonitor.meter("fatalFailures")

    delivery: Delivery[Bytes] =>
      try {
        // we try to catch also long-lasting synchronous work on the thread
        val action = Task { userReadAction(delivery) }
          .executeOn(blockingScheduler)
          .asyncBoundary
          .map(Task.fromEffect[F, DeliveryResult])
          .flatten

        val timedOutAction = if (processTimeout == Duration.ZERO) {
          action
        } else {
          action
            .timeout(ScalaDuration(processTimeout.toMillis, TimeUnit.MILLISECONDS))
            .onErrorRecoverWith {
              case e: TimeoutException => doTimeoutAction(consumerConfig, timeoutsMeter, e)
            }
        }

        timedOutAction
          .onErrorRecoverWith {
            case NonFatal(e) =>
              fatalFailuresMeter.mark()
              logger.warn(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
              Task.now(consumerConfig.failureAction)
          }
          .to[F]

      } catch {
        case NonFatal(e) =>
          fatalFailuresMeter.mark()
          logger.error(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
          Effect[F].pure(consumerConfig.failureAction)
      }
  }

  private def doTimeoutAction[A, F[_]: Effect](consumerConfig: ConsumerConfig,
                                               timeoutsMeter: Meter,
                                               e: TimeoutException): Task[DeliveryResult] = Task {
    import consumerConfig._

    timeoutsMeter.mark()

    lazy val msg = s"[$name] Task timed-out, applying DeliveryResult.${consumerConfig.timeoutAction}"

    timeoutLogLevel match {
      case Level.ERROR => logger.error(msg, e)
      case Level.WARN => logger.warn(msg, e)
      case Level.INFO => logger.info(msg, e)
      case Level.DEBUG => logger.debug(msg, e)
      case Level.TRACE => logger.trace(msg, e)
    }

    consumerConfig.timeoutAction
  }

  implicit class WrapConfig(val c: Config) extends AnyVal {
    def wrapped(prefix: String = "root"): Config = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      ConfigFactory
        .empty()
        .withValue(prefix, c.withFallback(ProducerDefaultConfig).root())
    }
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.mapValues(_.asInstanceOf[Object]).asJava
  }

}
