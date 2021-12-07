package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{Delivery => _}
import org.slf4j.event.Level

import scala.concurrent.duration.FiniteDuration

class DefaultRabbitMQConsumer[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
    private[rabbitmq] val base: ConsumerBase[F, A],
    processTimeout: FiniteDuration,
    timeoutAction: DeliveryResult,
    timeoutLogLevel: Level,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener)(userAction: DeliveryReadAction[F, A])
    extends ConsumerWithCallbackBase(base, failureAction, consumerListener)
    with RabbitMQConsumer[F] {
  import base._

  private val timeoutDelivery = watchForTimeoutIfConfigured(processTimeout, timeoutAction, timeoutLogLevel) _

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
