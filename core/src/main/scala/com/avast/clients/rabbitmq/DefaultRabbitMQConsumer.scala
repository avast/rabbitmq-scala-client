package com.avast.clients.rabbitmq

import cats.effect._
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Delivery => _, _}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds

class DefaultRabbitMQConsumer[F[_]: Effect](
    override val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    override protected val connectionInfo: RabbitMQConnectionInfo,
    override protected val monitor: Monitor,
    failureAction: DeliveryResult,
    consumerListener: ConsumerListener,
    override protected val republishStrategy: RepublishStrategy,
    override protected val blocker: Blocker)(readAction: DeliveryReadAction[F, Bytes])(implicit override protected val cs: ContextShift[F])
    extends ConsumerWithCallbackBase(channel, failureAction, consumerListener)
    with RabbitMQConsumer[F]
    with ConsumerBase[F]
    with StrictLogging {

  import DefaultRabbitMQConsumer._

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    processingCount.incrementAndGet()

    val deliveryTag = envelope.getDeliveryTag
    val messageId = properties.getMessageId
    val routingKey = properties.getHeaderValue(RepublishOriginalRoutingKeyHeaderName).getOrElse(envelope.getRoutingKey)

    val action = handleDelivery(messageId, deliveryTag, properties, routingKey, body)(readAction)
      .flatTap(_ =>
        F.delay {
          processingCount.decrementAndGet()
          logger.debug(s"Delivery processed successfully (tag $deliveryTag)")
      })
      .recoverWith {
        case e =>
          F.delay {
            processingCount.decrementAndGet()
            processingFailedMeter.mark()
            logger.debug("Could not process delivery", e)
          } >>
            F.raiseError(e)
      }

    Effect[F].toIO(action).unsafeToFuture() // actually start the processing

    ()
  }
}

object DefaultRabbitMQConsumer {
  final val RepublishOriginalRoutingKeyHeaderName = "X-Original-Routing-Key".toLowerCase
  final val RepublishOriginalUserId = "X-Original-User-Id".toLowerCase
}
