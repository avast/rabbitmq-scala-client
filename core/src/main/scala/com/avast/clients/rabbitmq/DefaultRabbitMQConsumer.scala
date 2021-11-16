package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{Delivery => _}
import org.slf4j.event.Level

import scala.concurrent.duration.FiniteDuration

class DefaultRabbitMQConsumer[F[_]: ConcurrentEffect: Timer, A: DeliveryConverter](
    private[rabbitmq] val base: ConsumerBase[F, A],
    channelOps: ConsumerChannelOps[F, A],
    processTimeout: FiniteDuration,
    timeoutAction: DeliveryResult,
    timeoutLogLevel: Level,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener[F])(userAction: DeliveryReadAction[F, A])
    extends ConsumerWithCallbackBase(base, channelOps, failureAction, consumerListener)
    with RabbitMQConsumer[F] {
  import base._

  override protected def handleNewDelivery(d: DeliveryWithContext[A]): F[Option[ConfirmedDeliveryResult[F]]] = {
    import d._

    // we try to catch also long-lasting synchronous work on the thread
    val resultAction = blocker.delay { userAction(delivery).map(ConfirmedDeliveryResult(_)) }.flatten

    watchForTimeoutIfConfigured(processTimeout, timeoutAction, timeoutLogLevel)(delivery, resultAction)(F.unit).map(Some(_))
  }

}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key"
  final val RepublishOriginalUserId = "X-Original-User-Id"
  final val FederationOriginalRoutingKeyHeaderName = "x-original-routing-key"
  final val CorrelationIdHeaderName = CorrelationIdStrategy.CorrelationIdKeyName
}
