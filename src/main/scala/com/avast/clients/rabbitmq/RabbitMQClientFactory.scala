package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util
import java.util.concurrent.ScheduledExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.{RabbitMQConsumer, RabbitMQProducer}
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

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object RabbitMQClientFactory extends LazyLogging {

  private[rabbitmq] final val ProducerRootConfigKey = "ffRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)

  private[rabbitmq] final val ConsumerRootConfigKey = "ffRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)

  private[rabbitmq] final val ConsumerBindingRootConfigKey = "ffRabbitMQConsumerBindingDefaults"
  private[rabbitmq] final val ConsumerBindingDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerBindingRootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = new ValueReader[Duration] {
    override def read(config: Config, path: String): Duration = config.getDuration(path)
  }

  private implicit final val JavaPathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }

  private implicit final val DeliveryResultReader: ValueReader[DeliveryResult] = new ValueReader[DeliveryResult] {

    import DeliveryResult._

    override def read(config: Config, path: String): DeliveryResult = config.getString(path).toLowerCase match {
      case "ack" => Ack
      case "reject" => Reject
      case "retry" => Retry
      case "republish" => Republish
    }
  }

  object Producer {

    /** Creates new instance of producer, using the passed TypeSafe configuration.
      *
      * @param providedConfig The configuration.
      * @param channelFactory See [[RabbitMQChannelFactory]].
      * @param monitor        Monitor for metrics.
      */
    def fromConfig(providedConfig: Config, channelFactory: RabbitMQChannelFactory, monitor: Monitor): RabbitMQProducer = {
      val producerConfig = providedConfig.wrapped.as[ProducerConfig]("root")

      create(producerConfig, channelFactory, monitor)
    }

    /** Creates new instance of producer, using the passed configuration.
      *
      * @param producerConfig The configuration.
      * @param channelFactory See [[RabbitMQChannelFactory]].
      * @param monitor        Monitor for metrics.
      */
    def create(producerConfig: ProducerConfig, channelFactory: RabbitMQChannelFactory, monitor: Monitor): DefaultRabbitMQProducer = {
      val channel = channelFactory.createChannel()

      prepareProducer(producerConfig, channel, channelFactory.info, monitor)
    }
  }

  object Consumer {

    /** Creates new instance of consumer, using the passed TypeSafe configuration.
      *
      * @param providedConfig           The configuration.
      * @param channelFactory           See [[RabbitMQChannelFactory]].
      * @param monitor                  Monitor for metrics.
      * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks (after specified timeout).
      * @param readAction               Action executed for each delivered message. You should never return a failed future.
      * @param ec                       [[ExecutionContext]] used for callbacks.
      */
    def fromConfig(providedConfig: Config,
                   channelFactory: RabbitMQChannelFactory,
                   monitor: Monitor,
                   scheduledExecutorService: ScheduledExecutorService = FutureTimeouter.Implicits.DefaultScheduledExecutor)(
        readAction: Delivery => Future[DeliveryResult])(implicit ec: ExecutionContext): RabbitMQConsumer = {

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

      create(consumerConfig, channelFactory, monitor, scheduledExecutorService)(readAction)
    }

    /** Creates new instance of consumer, using the passed configuration.
      *
      * @param consumerConfig           The configuration.
      * @param channelFactory           See [[RabbitMQChannelFactory]].
      * @param monitor                  Monitor for metrics.
      * @param scheduledExecutorService [[ScheduledExecutorService]] used for timeouting tasks (after specified timeout).
      * @param readAction               Action executed for each delivered message. You should never return a failed future.
      * @param ec                       [[ExecutionContext]] used for callbacks.
      */
    def create(consumerConfig: ConsumerConfig,
               channelFactory: RabbitMQChannelFactory,
               monitor: Monitor,
               scheduledExecutorService: ScheduledExecutorService)(readAction: (Delivery) => Future[DeliveryResult])(
        implicit ec: ExecutionContext): RabbitMQConsumer = {
      val channel = channelFactory.createChannel()

      prepareConsumer(consumerConfig, readAction, channelFactory.info, channel, monitor, scheduledExecutorService)
    }
  }

  private def prepareProducer(producerConfig: ProducerConfig,
                              channel: ServerChannel,
                              channelFactoryInfo: RabbitMqChannelFactoryInfo,
                              monitor: Monitor): DefaultRabbitMQProducer = {
    import producerConfig._

    // auto declare of exchange
    // parse it only if it's needed
    if (declare.getBoolean("enabled")) {
      val d = declare.wrapped.as[AutoDeclareExchange]("root")

      declareExchange(exchange, channelFactoryInfo, channel, d)
    }

    new DefaultRabbitMQProducer(producerConfig.name, exchange, channel, useKluzo, reportUnroutable, monitor)
  }

  private def declareExchange(name: String,
                              channelFactoryInfo: RabbitMqChannelFactoryInfo,
                              channel: ServerChannel,
                              autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${channelFactoryInfo.virtualHost}'")
      channel.exchangeDeclare(name, `type`, durable, autoDelete, null)
    }
    ()
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              readAction: (Delivery) => Future[DeliveryResult],
                              channelFactoryInfo: RabbitMqChannelFactoryInfo,
                              channel: ServerChannel,
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
        declareQueue(channel, queueName = queueName, durable = durable, exclusive = exclusive, autoDelete = autoDelete)
      }
    }

    // set prefetch size (per consumer)
    channel.basicQos(consumerConfig.prefetchCount)

    // auto bind
    bindQueues(channelFactoryInfo, channel, consumerConfig)

    prepareConsumer(consumerConfig, channelFactoryInfo, channel, readAction, monitor, scheduledExecutor)(ec)
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean): Queue.DeclareOk = {
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, new util.HashMap())
  }

  private def bindQueues(channelFactoryInfo: RabbitMqChannelFactoryInfo, channel: ServerChannel, consumerConfig: ConsumerConfig): Unit = {
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

  private[rabbitmq] def bindQueue(channelFactoryInfo: RabbitMqChannelFactoryInfo)(channel: ServerChannel, queueName: String)(
      exchangeName: String,
      routingKey: String): AMQP.Queue.BindOk = {
    logger.info(s"Binding $exchangeName($routingKey) -> '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")

    channel.queueBind(queueName, exchangeName, routingKey)
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              channelFactoryInfo: RabbitMqChannelFactoryInfo,
                              channel: ServerChannel,
                              userReadAction: Delivery => Future[DeliveryResult],
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(ec: ExecutionContext): RabbitMQConsumer = {
    import consumerConfig._

    val finalExecutor = if (useKluzo) {
      Continuity.wrapExecutionContext(ec)
    } else {
      ec
    }

    val readAction = wrapReadAction(consumerConfig, userReadAction, finalExecutor, scheduledExecutor)(finalExecutor)

    val consumer =
      new DefaultRabbitMQConsumer(name,
                                  channel,
                                  queueName,
                                  useKluzo,
                                  monitor,
                                  failureAction,
                                  bindQueue(channelFactoryInfo)(channel, queueName))(readAction)(finalExecutor)

    val tag = if (consumerTag == "Default") "" else consumerTag

    channel.basicConsume(queueName, false, tag, consumer)

    consumer
  }

  private def wrapReadAction(
      consumerConfig: ConsumerConfig,
      userReadAction: Delivery => Future[DeliveryResult],
      finalExecutor: ExecutionContext,
      scheduledExecutor: ScheduledExecutorService)(implicit ec: ExecutionContext): (Delivery) => Future[DeliveryResult] = {
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
            case NonFatal(e) =>
              traceId.foreach(Kluzo.setTraceId)

              logger.warn("Error while executing callback, will be redelivered", e)
              DeliveryResult.Retry
          }(finalExecutor)
      } catch {
        case NonFatal(e) =>
          logger.error("Error while executing callback, will be redelivered", e)
          Future.successful(DeliveryResult.Retry)
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

case class ConsumerConfig(queueName: String,
                          processTimeout: Duration,
                          failureAction: DeliveryResult,
                          prefetchCount: Int,
                          useKluzo: Boolean,
                          declare: AutoDeclareQueue,
                          bindings: immutable.Seq[AutoBindQueue],
                          consumerTag: String,
                          name: String)

case class AutoDeclareQueue(enabled: Boolean, durable: Boolean, exclusive: Boolean, autoDelete: Boolean)

case class AutoBindQueue(exchange: BindExchange, routingKeys: immutable.Seq[String])

case class BindExchange(name: String, declare: Config)

case class ProducerConfig(exchange: String, declare: Config, useKluzo: Boolean, reportUnroutable: Boolean, name: String)

case class AutoDeclareExchange(enabled: Boolean, `type`: String, durable: Boolean, autoDelete: Boolean)
