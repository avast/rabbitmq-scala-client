package com.avast.clients.rabbitmq

import java.time.Duration
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Reject, Republish, Retry}
import com.avast.clients.rabbitmq.api._
import com.avast.continuity.monix.Monix
import com.avast.kluzo.Kluzo
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.Queue
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.language.{higherKinds, implicitConversions}
import scala.util.control.NonFatal

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  private type DefaultDeliveryReadAction = DeliveryReadAction[Task, Bytes]

  private[rabbitmq] final val DeclareQueueRootConfigKey = "ffRabbitMQDeclareQueueDefaults"
  private[rabbitmq] final val DeclareQueueDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareQueueRootConfigKey)

  private[rabbitmq] final val BindExchangeRootConfigKey = "ffRabbitMQBindExchangeDefaults"
  private[rabbitmq] final val BindExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(BindExchangeRootConfigKey)

  private[rabbitmq] final val DeclareExchangeRootConfigKey = "ffRabbitMQDeclareExchangeDefaults"
  private[rabbitmq] final val DeclareExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareExchangeRootConfigKey)

  private[rabbitmq] final val ProducerRootConfigKey = "ffRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareExchangeDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerRootConfigKey = "ffRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareQueueDefaultConfig).root())
  }
  private[rabbitmq] final val ManualConsumerRootConfigKey = "ffRabbitMQManualConsumerDefaults"
  private[rabbitmq] final val ManualConsumerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareQueueDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerBindingRootConfigKey = "ffRabbitMQConsumerBindingDefaults"
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

  object Producer {

    def fromConfig[F[_]: FromTask, A: ProductConverter](providedConfig: Config,
                                                        channel: ServerChannel,
                                                        factoryInfo: RabbitMQConnectionInfo,
                                                        blockingScheduler: Scheduler,
                                                        monitor: Monitor): DefaultRabbitMQProducer[F, A] = {
      val producerConfig = providedConfig.wrapped.as[ProducerConfig]("root")

      create[F, A](producerConfig, channel, factoryInfo, blockingScheduler, monitor)
    }

    def create[F[_]: FromTask, A: ProductConverter](producerConfig: ProducerConfig,
                                                    channel: ServerChannel,
                                                    factoryInfo: RabbitMQConnectionInfo,
                                                    blockingScheduler: Scheduler,
                                                    monitor: Monitor): DefaultRabbitMQProducer[F, A] = {

      prepareProducer[F, A](producerConfig, channel, factoryInfo, blockingScheduler, monitor)
    }
  }

  object Consumer {

    def fromConfig[F[_]: ToTask, A: DeliveryConverter](providedConfig: Config,
                                                       channel: ServerChannel,
                                                       channelFactoryInfo: RabbitMQConnectionInfo,
                                                       blockingScheduler: Scheduler,
                                                       monitor: Monitor,
                                                       consumerListener: ConsumerListener)(readAction: DeliveryReadAction[F, A])(
        implicit scheduler: Scheduler): DefaultRabbitMQConsumer = {

      val mergedConfig = providedConfig.withFallback(ConsumerDefaultConfig)

      // merge consumer binding defaults
      val updatedConfig = {
        val updated = mergedConfig.as[Seq[Config]]("bindings").map { bindConfig =>
          bindConfig.withFallback(ConsumerBindingDefaultConfig).root()
        }

        import scala.collection.JavaConverters._

        mergedConfig.withValue("bindings", ConfigValueFactory.fromIterable(updated.asJava))
      }

      val consumerConfig = updatedConfig.wrapped.as[ConsumerConfig]("root")

      create[F, A](consumerConfig, channel, channelFactoryInfo, blockingScheduler, monitor, consumerListener)(readAction)
    }

    def create[F[_]: ToTask, A: DeliveryConverter](consumerConfig: ConsumerConfig,
                                                   channel: ServerChannel,
                                                   channelFactoryInfo: RabbitMQConnectionInfo,
                                                   blockingScheduler: Scheduler,
                                                   monitor: Monitor,
                                                   consumerListener: ConsumerListener)(readAction: DeliveryReadAction[F, A])(
        implicit scheduler: Scheduler): DefaultRabbitMQConsumer = {

      prepareConsumer(consumerConfig, readAction, channelFactoryInfo, channel, consumerListener, blockingScheduler, monitor)
    }
  }

  object ManualConsumer {

    def fromConfig[F[_]: FromTask, A: DeliveryConverter](
        providedConfig: Config,
        channel: ServerChannel,
        channelFactoryInfo: RabbitMQConnectionInfo,
        blockingScheduler: Scheduler,
        monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQManualConsumer[F, A] = {

      val mergedConfig = providedConfig.withFallback(ConsumerDefaultConfig)

      // merge consumer binding defaults
      val updatedConfig = {
        val updated = mergedConfig.as[Seq[Config]]("bindings").map { bindConfig =>
          bindConfig.withFallback(ConsumerBindingDefaultConfig).root()
        }

        import scala.collection.JavaConverters._

        mergedConfig.withValue("bindings", ConfigValueFactory.fromIterable(updated.asJava))
      }

      val consumerConfig = updatedConfig.wrapped.as[ManualConsumerConfig]("root")

      create[F, A](consumerConfig, channel, channelFactoryInfo, blockingScheduler, monitor)
    }

    def create[F[_]: FromTask, A: DeliveryConverter](
        consumerConfig: ManualConsumerConfig,
        channel: ServerChannel,
        channelFactoryInfo: RabbitMQConnectionInfo,
        blockingScheduler: Scheduler,
        monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQManualConsumer[F, A] = {

      prepareManualConsumer(consumerConfig, channelFactoryInfo, channel, blockingScheduler, monitor)
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
      bindQueue(config.withFallback(ConsumerBindingDefaultConfig).as[BindQueue], channel, channelFactoryInfo)
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

      routingKeys.foreach {
        DefaultRabbitMQClientFactory.this.bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, _, bindArguments.value)
      }
    }

    private def bindExchange(config: BindExchange, channel: ServerChannel, channelFactoryInfo: RabbitMQConnectionInfo): Task[Unit] = Task {
      import config._

      routingKeys.foreach {
        DefaultRabbitMQClientFactory.this
          .bindExchange(channelFactoryInfo)(channel, sourceExchangeName, destExchangeName, arguments.value)
      }
    }
  }

  private def prepareProducer[F[_]: FromTask, A: ProductConverter](producerConfig: ProducerConfig,
                                                                   channel: ServerChannel,
                                                                   channelFactoryInfo: RabbitMQConnectionInfo,
                                                                   blockingScheduler: Scheduler,
                                                                   monitor: Monitor): DefaultRabbitMQProducer[F, A] = {
    import producerConfig._

    val finalBlockingScheduler = if (useKluzo) Monix.wrapScheduler(blockingScheduler) else blockingScheduler

    // auto declare of exchange
    // parse it only if it's needed
    // "Lazy" parsing, because exchange type is not part of reference.conf and we don't want to make it fail on missing type when enabled=false
    if (declare.getBoolean("enabled")) {
      val d = declare.wrapped.as[AutoDeclareExchange]("root")
      declareExchange(exchange, channelFactoryInfo, channel, d)
    }
    new DefaultRabbitMQProducer[F, A](producerConfig.name, exchange, channel, useKluzo, reportUnroutable, finalBlockingScheduler, monitor)
  }

  private[rabbitmq] def declareExchange(name: String,
                                        channelFactoryInfo: RabbitMQConnectionInfo,
                                        channel: ServerChannel,
                                        autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      declareExchange(name, `type`, durable, autoDelete, arguments, channel, channelFactoryInfo)
    }
    ()
  }

  private def declareExchange(name: String,
                              `type`: String,
                              durable: Boolean,
                              autoDelete: Boolean,
                              arguments: DeclareArguments,
                              channel: ServerChannel,
                              channelFactoryInfo: RabbitMQConnectionInfo): Unit = {
    logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${channelFactoryInfo.virtualHost}'")
    val javaArguments = argsAsJava(arguments.value)
    channel.exchangeDeclare(name, `type`, durable, autoDelete, javaArguments)
    ()
  }

  private def prepareConsumer[F[_]: ToTask, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      readAction: DeliveryReadAction[F, A],
      channelFactoryInfo: RabbitMQConnectionInfo,
      channel: ServerChannel,
      consumerListener: ConsumerListener,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQConsumer = {

    // auto declare exchanges
    consumerConfig.bindings.foreach { bind =>
      import bind.exchange._

      // parse it only if it's needed
      if (declare.getBoolean("enabled")) {
        val d = declare.wrapped.as[AutoDeclareExchange]("root")

        declareExchange(name, channelFactoryInfo, channel, d)
      }
    }

    // auto declare queue
    {
      import consumerConfig.declare._
      import consumerConfig.queueName

      if (enabled) {
        logger.info(s"Declaring queue '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")
        declareQueue(channel, queueName, durable, exclusive, autoDelete, arguments)
      }
    }

    // set prefetch size (per consumer)
    channel.basicQos(consumerConfig.prefetchCount)

    // auto bind
    bindQueues(channelFactoryInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    prepareConsumer(consumerConfig, channelFactoryInfo, channel, readAction, consumerListener, blockingScheduler, monitor)
  }

  private def prepareManualConsumer[F[_]: FromTask, A: DeliveryConverter](
      consumerConfig: ManualConsumerConfig,
      channelFactoryInfo: RabbitMQConnectionInfo,
      channel: ServerChannel,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQManualConsumer[F, A] = {

    import consumerConfig._

    // auto declare exchanges
    consumerConfig.bindings.foreach { bind =>
      import bind.exchange._

      // parse it only if it's needed
      if (declare.getBoolean("enabled")) {
        val d = declare.wrapped.as[AutoDeclareExchange]("root")

        declareExchange(name, channelFactoryInfo, channel, d)
      }
    }

    // auto declare queue
    {
      import consumerConfig.declare._

      if (enabled) {
        logger.info(s"Declaring queue '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")
        declareQueue(channel, queueName, durable, exclusive, autoDelete, arguments)
      }
    }

    // auto bind
    bindQueues(channelFactoryInfo, channel, consumerConfig.queueName, consumerConfig.bindings)

    val finalBlockingScheduler: Scheduler = if (useKluzo) {
      Monix.wrapScheduler(scheduler)
    } else {
      scheduler
    }

    new DefaultRabbitMQManualConsumer[F, A](name, channel, queueName, failureAction, monitor, finalBlockingScheduler)
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean,
                                     arguments: DeclareArguments): Queue.DeclareOk = {
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.value)
  }

  private def bindQueues(channelFactoryInfo: RabbitMQConnectionInfo,
                         channel: ServerChannel,
                         queueName: String,
                         bindings: immutable.Seq[AutoBindQueue]): Unit = {
    bindings.foreach { bind =>
      import bind._
      val exchangeName = bind.exchange.name

      if (routingKeys.nonEmpty) {
        routingKeys.foreach { routingKey =>
          bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, routingKey, bindArguments.value)
        }
      } else {
        // binding without routing key, possibly to fanout exchange

        bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, "", bindArguments.value)
      }
    }
  }

  private[rabbitmq] def bindQueue(channelFactoryInfo: RabbitMQConnectionInfo)(
      channel: ServerChannel,
      queueName: String)(exchangeName: String, routingKey: String, arguments: ArgumentsMap): AMQP.Queue.BindOk = {
    logger.info(s"Binding exchange $exchangeName($routingKey) -> queue '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")

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

  private def prepareConsumer[F[_]: ToTask, A: DeliveryConverter](
      consumerConfig: ConsumerConfig,
      channelFactoryInfo: RabbitMQConnectionInfo,
      channel: ServerChannel,
      userReadAction: DeliveryReadAction[F, A],
      consumerListener: ConsumerListener,
      blockingScheduler: Scheduler,
      monitor: Monitor)(implicit scheduler: Scheduler): DefaultRabbitMQConsumer = {
    import consumerConfig._

    val finalBlockingScheduler: Scheduler = if (useKluzo) {
      Monix.wrapScheduler(scheduler)
    } else {
      scheduler
    }

    val readAction: DefaultDeliveryReadAction = {
      val convAction: DefaultDeliveryReadAction = { (d: Delivery[Bytes]) =>
        try {
          implicitly[DeliveryConverter[A]].convert(d.body) match {
            case Right(a) =>
              val devA = d.copy(body = a)
              implicitly[ToTask[F]].apply(userReadAction(devA))

            case Left(ce) => Task.raiseError(ce)
          }
        } catch {
          case NonFatal(e) =>
            Task.raiseError(e)
        }
      }

      wrapReadAction(consumerConfig, convAction, blockingScheduler)
    }

    val consumer =
      new DefaultRabbitMQConsumer(name, channel, queueName, useKluzo, monitor, failureAction, consumerListener, blockingScheduler)(
        readAction)(finalBlockingScheduler)

    val finalConsumerTag = if (consumerTag == "Default") "" else consumerTag

    channel.basicConsume(queueName, false, finalConsumerTag, consumer)

    consumer
  }

  private def wrapReadAction[A](consumerConfig: ConsumerConfig, userReadAction: DefaultDeliveryReadAction, blockingScheduler: Scheduler)(
      implicit callbackScheduler: Scheduler): DefaultDeliveryReadAction = {
    import consumerConfig._

    (delivery: Delivery[Bytes]) =>
      try {
        // we try to catch also long-lasting synchronous work on the thread
        val action = Task.deferFuture {
          Future {
            userReadAction(delivery)
          }(blockingScheduler)
        }.flatten

        val traceId = Kluzo.getTraceId

        action
          .timeout(ScalaDuration(processTimeout.toMillis, TimeUnit.MILLISECONDS))
          .onErrorRecover {
            case e: TimeoutException =>
              traceId.foreach(Kluzo.setTraceId)

              logger.warn(s"[$name] Task timed-out, applying DeliveryResult.${consumerConfig.timeoutAction}", e)
              consumerConfig.timeoutAction

            case NonFatal(e) =>
              traceId.foreach(Kluzo.setTraceId)

              logger.warn(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
              consumerConfig.failureAction
          }
          .executeOn(callbackScheduler)
      } catch {
        case NonFatal(e) =>
          logger.error(s"[$name] Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
          Task.now(consumerConfig.failureAction)
      }

  }

  implicit class WrapConfig(val c: Config) extends AnyVal {
    def wrapped: Config = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      ConfigFactory
        .empty()
        .withValue("root", c.withFallback(ProducerDefaultConfig).root())
    }
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.mapValues(_.asInstanceOf[Object]).asJava
  }

}
