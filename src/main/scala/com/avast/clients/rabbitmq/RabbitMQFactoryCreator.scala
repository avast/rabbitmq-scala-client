//package com.avast.clients.rabbitmq
//
//import com.avast.clients.rabbitmq.api.{RabbitMQConsumer, RabbitMQProducer}
//import com.avast.metrics.scalaapi.Monitor
//import com.typesafe.config.Config
//import com.typesafe.scalalogging.StrictLogging
//
//import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
//
//class RabbitMQFactoryCreator(rootConfig: Config, monitor: Monitor, connectionEc: ExecutionContextExecutorService) extends StrictLogging {
//
//  def create(connectionName: String): RabbitMQFactory = {
//    logger.info(s"Creating RabbitMQ factory with connection '$connectionName'")
//
//    new RabbitMQFactory(rootConfig.getConfig(connectionName), monitor.named(connectionName), connectionEc)
//  }
//}
//
//class RabbitMQFactory(connectionConfig: Config, monitor: Monitor, connectionEc: ExecutionContextExecutorService)
//    extends AutoCloseable
//    with StrictLogging {
//  private val channelFactory = RabbitMQChannelFactory.fromConfig(connectionConfig, Some(connectionEc))
//
//  // scalastyle:off
//  private var closeables = Seq[AutoCloseable]()
//  // scalastyle:on
//  /**
//    * See [[RabbitMQClientFactory.Consumer.fromConfig]].
//    *
//    * @param name Name of the consumer in config.
//    */
//  def newConsumer(name: String, executionContext: ExecutionContext)(readAction: Delivery => Future[DeliveryResult]): RabbitMQConsumer = {
//    val config = connectionConfig.getConfig(name)
//    logger.info(s"Creating RabbitMQ consumer with name '${config.getString("name")}'")
//
//    val consumer =
//      RabbitMQClientFactory.Consumer.fromConfig(config, channelFactory, monitor.named(name))(readAction)(executionContext)
//
//    closeables.synchronized {
//      closeables = closeables :+ consumer
//    }
//
//    consumer
//  }
//
//  /**
//    * See [[RabbitMQClientFactory.Producer.fromConfig]].
//    *
//    * @param name Name of the producer in config.
//    */
//  def newProducer(name: String): RabbitMQProducer = {
//    val config = connectionConfig.getConfig(name)
//    logger.info(s"Creating RabbitMQ producer with name '${config.getString("name")}'")
//
//    val producer = RabbitMQClientFactory.Producer.fromConfig(config, channelFactory, monitor.named(name))
//
//    closeables.synchronized {
//      closeables = closeables :+ producer
//    }
//
//    producer
//  }
//
//  override def close(): Unit = {
//    closeables.synchronized {
//      closeables.foreach(_.close())
//    }
//  }
//}
