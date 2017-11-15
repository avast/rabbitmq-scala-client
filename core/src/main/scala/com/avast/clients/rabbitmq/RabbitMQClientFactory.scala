package com.avast.clients.rabbitmq

import java.time.Duration
import java.util.concurrent.{ScheduledExecutorService, TimeoutException}

import com.avast.clients.rabbitmq.RabbitMQFactory.ServerChannel
import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Reject, Republish, Retry}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, RabbitMQConsumer, RabbitMQProducer}
import com.avast.continuity.Continuity
import com.avast.kluzo.Kluzo
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.errorhandling.FutureTimeouter
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.Queue
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[rabbitmq] object RabbitMQClientFactory extends LazyLogging {

  private[rabbitmq] final val ProducerRootConfigKey = "ffRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)

  private[rabbitmq] final val ConsumerRootConfigKey = "ffRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)

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

  private implicit final val rabbitArgumentsReader: ValueReader[DeclareArguments] = (config: Config, path: String) => {
    import scala.collection.JavaConverters._
    val argumentsMap = config
      .getObject(path)
      .asScala
      .toMap
      .mapValues(_.unwrapped())

    DeclareArguments(argumentsMap)
  }

  object Producer {

    def fromConfig(providedConfig: Config, channel: ServerChannel, factoryInfo: RabbitMqFactoryInfo, monitor: Monitor): RabbitMQProducer = {
      val producerConfig = providedConfig.wrapped.as[ProducerConfig]("root")

      create(producerConfig, channel, factoryInfo, monitor)
    }

    def create(producerConfig: ProducerConfig,
               channel: ServerChannel,
               factoryInfo: RabbitMqFactoryInfo,
               monitor: Monitor): DefaultRabbitMQProducer = {

      prepareProducer(producerConfig, channel, factoryInfo, monitor)
    }
  }

  object Consumer {

    def fromConfig(providedConfig: Config,
                   channel: ServerChannel,
                   channelFactoryInfo: RabbitMqFactoryInfo,
                   monitor: Monitor,
                   consumerListener: ConsumerListener,
                   scheduledExecutorService: ScheduledExecutorService)(readAction: Delivery => Future[DeliveryResult])(
        implicit ec: ExecutionContext): RabbitMQConsumer = {

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

      create(consumerConfig, channel, channelFactoryInfo, monitor, consumerListener, scheduledExecutorService)(readAction)
    }

    def create(consumerConfig: ConsumerConfig,
               channel: ServerChannel,
               channelFactoryInfo: RabbitMqFactoryInfo,
               monitor: Monitor,
               consumerListener: ConsumerListener,
               scheduledExecutorService: ScheduledExecutorService)(readAction: (Delivery) => Future[DeliveryResult])(
        implicit ec: ExecutionContext): RabbitMQConsumer = {

      prepareConsumer(consumerConfig, readAction, channelFactoryInfo, channel, consumerListener, monitor, scheduledExecutorService)
    }
  }

  private def prepareProducer(producerConfig: ProducerConfig,
                              channel: ServerChannel,
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              monitor: Monitor): DefaultRabbitMQProducer = {
    import producerConfig._

    // auto declare of exchange
    // parse it only if it's needed
    // "Lazy" parsing, because exchange type is not part of reference.conf and we don't want to make it fail on missing type when enabled=false
    if (declare.getBoolean("enabled")) {
      val d = declare.wrapped.as[AutoDeclareExchange]("root")
      declareExchange(exchange, channelFactoryInfo, channel, d)
    }
    new DefaultRabbitMQProducer(producerConfig.name, exchange, channel, useKluzo, reportUnroutable, monitor)
  }

  private def declareExchange(name: String,
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              channel: ServerChannel,
                              autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${channelFactoryInfo.virtualHost}'")
      import scala.collection.JavaConverters._
      val javaArguments = arguments.value.mapValues(_.asInstanceOf[Object]).asJava
      channel.exchangeDeclare(name, `type`, durable, autoDelete, javaArguments)
    }
    ()
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              readAction: (Delivery) => Future[DeliveryResult],
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              channel: ServerChannel,
                              consumerListener: ConsumerListener,
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(implicit ec: ExecutionContext): RabbitMQConsumer = {

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
    bindQueues(channelFactoryInfo, channel, consumerConfig)

    prepareConsumer(consumerConfig, channelFactoryInfo, channel, readAction, consumerListener, monitor, scheduledExecutor)(ec)
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean,
                                     arguments: DeclareArguments): Queue.DeclareOk = {
    import scala.collection.JavaConverters._
    val javaArguments = arguments.value.mapValues(_.asInstanceOf[Object]).asJava
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, javaArguments)
  }

  private def bindQueues(channelFactoryInfo: RabbitMqFactoryInfo, channel: ServerChannel, consumerConfig: ConsumerConfig): Unit = {
    import consumerConfig.queueName

    consumerConfig.bindings.foreach { bind =>
      import bind._
      val exchangeName = bind.exchange.name

      if (routingKeys.nonEmpty) {
        routingKeys.foreach { routingKey =>
          bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, routingKey)
        }
      } else {
        // binding without routing key, possibly to fanout exchange

        bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, "")
      }
    }
  }

  private[rabbitmq] def bindQueue(channelFactoryInfo: RabbitMqFactoryInfo)(channel: ServerChannel, queueName: String)(
      exchangeName: String,
      routingKey: String): AMQP.Queue.BindOk = {
    logger.info(s"Binding $exchangeName($routingKey) -> '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")

    channel.queueBind(queueName, exchangeName, routingKey)
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              channel: ServerChannel,
                              userReadAction: Delivery => Future[DeliveryResult],
                              consumerListener: ConsumerListener,
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(ec: ExecutionContext): RabbitMQConsumer = {
    import consumerConfig._

    implicit val finalExecutor: ExecutionContext = if (useKluzo) {
      Continuity.wrapExecutionContext(ec)
    } else {
      ec
    }

    val readAction = wrapReadAction(consumerConfig, userReadAction, scheduledExecutor)

    val consumer =
      new DefaultRabbitMQConsumer(name,
                                  channel,
                                  queueName,
                                  useKluzo,
                                  monitor,
                                  failureAction,
                                  consumerListener,
                                  bindQueue(channelFactoryInfo)(channel, queueName))(readAction)

    val tag = if (consumerTag == "Default") "" else consumerTag

    channel.basicConsume(queueName, false, tag, consumer)

    consumer
  }

  private def wrapReadAction(
      consumerConfig: ConsumerConfig,
      userReadAction: Delivery => Future[DeliveryResult],
      scheduledExecutor: ScheduledExecutorService)(implicit finalExecutor: ExecutionContext): (Delivery) => Future[DeliveryResult] = {
    import FutureTimeouter._
    import consumerConfig._

    (delivery: Delivery) =>
      try {
        // we try to catch also long-lasting synchronous work on the thread
        val action = Future {
          userReadAction(delivery)
        }.flatMap(identity)

        val traceId = Kluzo.getTraceId

        action
          .timeoutAfter(processTimeout)(finalExecutor, scheduledExecutor)
          .recover {
            case e: TimeoutException =>
              traceId.foreach(Kluzo.setTraceId)
              logger.warn(s"Task timed-out, applying DeliveryResult.${consumerConfig.timeoutAction}", e)
              consumerConfig.timeoutAction

            case NonFatal(e) =>
              traceId.foreach(Kluzo.setTraceId)

              logger.warn(s"Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
              consumerConfig.failureAction
          }(finalExecutor)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
          Future.successful(consumerConfig.failureAction)
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

}
