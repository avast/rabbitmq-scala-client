package com.avast.clients.rabbitmq

import java.nio.file.{Path, Paths}
import java.time.Duration
import java.util
import java.util.concurrent.ScheduledExecutorService

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.{RabbitMQConsumer, RabbitMQProducer}
import com.avast.metrics.api.Monitor
import com.avast.utils2.errorhandling.FutureTimeouter
import com.rabbitmq.client.AMQP
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object RabbitMQClientFactory extends LazyLogging {

  private[rabbitmq] final val ConsumerRootConfigKey = "ffRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)

  private[rabbitmq] final val ProducerRootConfigKey = "ffRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = new ValueReader[Duration] {
    override def read(config: Config, path: String): Duration = config.getDuration(path)
  }

  private implicit final val JavaPathReader: ValueReader[Path] = new ValueReader[Path] {
    override def read(config: Config, path: String): Path = Paths.get(config.getString(path))
  }

  object Producer {
    def fromConfig(providedConfig: Config, channelCreator: RabbitMQChannelFactory, monitor: Monitor): RabbitMQProducer = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      val config = ConfigFactory.empty()
        .withValue("root", providedConfig.withFallback(ProducerDefaultConfig).root())

      val producerConfig = config.as[ProducerConfig]("root")

      val channel = channelCreator.createChannel()

      prepareProducer(producerConfig, channel, monitor)
    }
  }

  object Consumer {
    def fromConfig(providedConfig: Config,
                   channelCreator: RabbitMQChannelFactory,
                   monitor: Monitor,
                   scheduledExecutorService: ScheduledExecutorService = FutureTimeouter.Implicits.DefaultScheduledExecutor)
                  (readAction: Delivery => Future[Boolean])
                  (implicit ec: ExecutionContext): RabbitMQConsumer = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      val config = ConfigFactory.empty()
        .withValue("root", providedConfig.withFallback(ConsumerDefaultConfig).root())


      val consumerConfig = config.as[ConsumerConfig]("root")

      val channel = channelCreator.createChannel()

      prepareConsumer(consumerConfig, readAction, channel, monitor, scheduledExecutorService)
    }
  }

  private def prepareProducer(producerConfig: ProducerConfig, channel: ServerChannel, monitor: Monitor): DefaultRabbitMQProducer = {
    import producerConfig._

    // auto declare
    declareExchange(exchange, channel, declare)

    new DefaultRabbitMQProducer(producerConfig.name, exchange, channel, monitor)
  }

  private def declareExchange(name: String, channel: ServerChannel, autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      logger.info(s"Declaring exchange '$name' of type ${`type`}")
      channel.exchangeDeclare(name, `type`, durable, autoDelete, new util.HashMap())
    }
    ()
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              readAction: (Delivery) => Future[Boolean],
                              channel: ServerChannel,
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(implicit ec: ExecutionContext): RabbitMQConsumer = {

    // auto declare exchange
    {
      import consumerConfig.bind.exchange._

      declareExchange(name, channel, declare)
    }

    // auto declare queue
    {
      import consumerConfig.declare._
      import consumerConfig.queueName

      if (enabled) {
        logger.info(s"Declaring queue '$queueName'")
        channel.queueDeclare(queueName, durable, exclusive, autoDelete, new util.HashMap())
      }
    }

    // auto bind
    {
      import consumerConfig.bind._
      import consumerConfig.queueName

      if (enabled) {
        val exchangeName = consumerConfig.bind.exchange.name

        routingKeys.foreach { routingKey =>
          bindTo(channel, queueName)(exchangeName, routingKey)
        }
      }
    }

    prepareConsumer(consumerConfig, channel, readAction, monitor, scheduledExecutor)
  }

  private def bindTo(channel: ServerChannel,queueName: String)(exchangeName: String, routingKey: String): AMQP.Queue.BindOk = {
    logger.info(s"Binding $exchangeName($routingKey) -> '$queueName'")
    channel.queueBind(queueName, exchangeName, routingKey)
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              channel: ServerChannel,
                              readAction: (Delivery) => Future[Boolean],
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)
                             (implicit ec: ExecutionContext): RabbitMQConsumer = {
    import FutureTimeouter._
    import consumerConfig._

    val consumer = new DefaultRabbitMQConsumer(name, channel, monitor, bindTo(channel, queueName))({ delivery =>
      try {
        readAction(delivery)
          .timeoutAfter(processTimeout)(ec, scheduledExecutor)
          .recover {
            case NonFatal(e) =>
              logger.warn("Error while executing callback, will be redelivered", e)
              false
          }
      } catch {
        case NonFatal(e) =>
          logger.error("Error while executing callback, will be redelivered", e)
          Future.successful(false)
      }
    })

    channel.basicConsume(queueName, false, consumer)

    consumer
  }

}


case class ConsumerConfig(queueName: String, processTimeout: Duration, declare: AutoDeclareQueue, bind: AutoBindQueue, name: String)

case class AutoDeclareQueue(enabled: Boolean, durable: Boolean, exclusive: Boolean, autoDelete: Boolean)

case class AutoBindQueue(enabled: Boolean, exchange: BindExchange, routingKeys: Seq[String])

case class BindExchange(name: String, declare: AutoDeclareExchange)

case class ProducerConfig(exchange: String, declare: AutoDeclareExchange, name: String)

case class AutoDeclareExchange(enabled: Boolean, `type`: String, durable: Boolean, autoDelete: Boolean)
