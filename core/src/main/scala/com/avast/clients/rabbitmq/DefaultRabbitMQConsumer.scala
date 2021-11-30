package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.clients.rabbitmq.DefaultRabbitMQClientFactory.watchForTimeoutIfConfigured
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.{Delivery => _}
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.event.Level

import scala.concurrent.duration.FiniteDuration

class DefaultRabbitMQConsumer[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
    override val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    override protected val connectionInfo: RabbitMQConnectionInfo,
    override protected val republishStrategy: RepublishStrategy,
    override protected val poisonedMessageHandler: PoisonedMessageHandler[F, A],
    processTimeout: FiniteDuration,
    timeoutAction: DeliveryResult,
    timeoutLogLevel: Level,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    override protected val monitor: Monitor,
    override protected val blocker: Blocker)(userAction: DeliveryReadAction[F, A])(implicit override protected val cs: ContextShift[F])
    extends ConsumerWithCallbackBase(channel, failureAction, consumerListener)
    with RabbitMQConsumer[F]
    with ConsumerBase[F, A]
    with StrictLogging {

  override protected val deliveryConverter: DeliveryConverter[A] = implicitly
  override protected implicit val F: Sync[F] = Sync[F]

  private val timeoutDelivery = watchForTimeoutIfConfigured[F, A](name, logger, monitor, processTimeout, timeoutAction, timeoutLogLevel) _

  override protected def handleNewDelivery(d: DeliveryWithMetadata[A]): F[DeliveryResult] = {
    import d._
    import metadata._

    val resultAction = blocker.delay { userAction(delivery) }.flatten // we try to catch also long-lasting synchronous work on the thread

    timeoutDelivery(delivery, messageId, correlationId, resultAction)(F.unit)
  }

}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key"
  final val RepublishOriginalUserId = "X-Original-User-Id"
  final val FederationOriginalRoutingKeyHeaderName = "x-original-routing-key"
  final val CorrelationIdHeaderName = "x-correlation-id"
}
