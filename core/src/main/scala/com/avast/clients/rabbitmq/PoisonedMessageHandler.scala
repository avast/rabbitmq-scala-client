package com.avast.clients.rabbitmq

import cats.Applicative
import cats.effect.{Resource, Sync, Timer}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFunctorOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.PoisonedMessageHandler.DiscardedTimeHeaderName
import com.avast.clients.rabbitmq.api.DeliveryResult._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.{Meter, Monitor}

import java.time.Instant
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

sealed trait PoisonedMessageHandler[F[_], A] {
  def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult]
}

private trait PoisonedMessageHandlerAction[F[_], A] {
  def handlePoisonedMessage(rawBody: Bytes)(delivery: Delivery[A], maxAttempts: Int)(implicit dctx: DeliveryContext): F[Unit]
}

private sealed abstract class PoisonedMessageHandlerBase[F[_]: Sync: Timer, A](maxAttempts: Int,
                                                                               republishDelay: Option[ExponentialDelay],
                                                                               helper: PoisonedMessageHandlerHelper[F])
    extends PoisonedMessageHandler[F, A]
    with PoisonedMessageHandlerAction[F, A] {

  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult] = {
    PoisonedMessageHandler.handleResult(delivery, rawBody, messageId, maxAttempts, helper, republishDelay, this)(result)
  }
}

private[rabbitmq] class LoggingPoisonedMessageHandler[F[_]: Sync: Timer, A](maxAttempts: Int,
                                                                            republishDelay: Option[ExponentialDelay],
                                                                            helper: PoisonedMessageHandlerHelper[F])
    extends PoisonedMessageHandlerBase[F, A](maxAttempts, republishDelay, helper) {
  import helper._

  override def handlePoisonedMessage(rawBody: Bytes)(delivery: Delivery[A], ma: Int)(implicit dctx: DeliveryContext): F[Unit] = {
    logger.warn(s"Message failures reached the limit $ma attempts, throwing away: ${redactIfConfigured(delivery)}")
  }
}

private[rabbitmq] class DeadQueuePoisonedMessageHandler[F[_]: Sync: Timer, A](
    maxAttempts: Int,
    republishDelay: Option[ExponentialDelay],
    helper: PoisonedMessageHandlerHelper[F])(moveToDeadQueue: (Delivery[A], Bytes, DeliveryContext) => F[Unit])
    extends PoisonedMessageHandlerBase[F, A](maxAttempts, republishDelay, helper) {
  import helper._

  override def handlePoisonedMessage(rawBody: Bytes)(delivery: Delivery[A], ma: Int)(implicit dctx: DeliveryContext): F[Unit] = {
    import dctx._

    logger.warn {
      s"Message $messageId failures reached the limit $ma attempts, moving it to the dead queue: ${redactIfConfigured(delivery)}"
    } >>
      moveToDeadQueue(delivery, rawBody, dctx) >>
      logger.debug(s"Message $messageId moved to the dead queue")
  }
}

private[rabbitmq] class NoOpPoisonedMessageHandler[F[_]: Sync, A](helper: PoisonedMessageHandlerHelper[F])
    extends PoisonedMessageHandler[F, A] {
  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult] = {
    result match {
      case DeliveryResult.DirectlyPoison =>
        helper.logger.warn("Delivery can't be poisoned, because NoOpPoisonedMessageHandler is installed! Rejecting instead...").as(Reject)

      case _ => Sync[F].pure(result)
    }
  }
}

private[rabbitmq] object DeadQueuePoisonedMessageHandler {
  def make[F[_]: Sync: Timer, A](conf: DeadQueuePoisonedMessageHandling,
                                 connection: RabbitMQConnection[F],
                                 helper: PoisonedMessageHandlerHelper[F]): Resource[F, DeadQueuePoisonedMessageHandler[F, A]] = {
    import conf._
    import helper._

    val pc = ProducerConfig(
      name = deadQueueProducer.name,
      exchange = deadQueueProducer.exchange,
      declare = deadQueueProducer.declare,
      reportUnroutable = deadQueueProducer.reportUnroutable,
      sizeLimitBytes = deadQueueProducer.sizeLimitBytes,
      properties = deadQueueProducer.properties
    )

    connection.newProducer[Bytes](pc, monitor.named("deadQueueProducer")).map { producer =>
      new DeadQueuePoisonedMessageHandler[F, A](maxAttempts, republishDelay, helper)(
        (d: Delivery[A], rawBody: Bytes, dctx: DeliveryContext) => {
          val cidStrategy = dctx.correlationId match {
            case Some(value) => CorrelationIdStrategy.Fixed(value.value)
            case None => CorrelationIdStrategy.RandomNew
          }

          val now = Instant.now()
          val finalProperties = d.properties.copy(headers = d.properties.headers.updated(DiscardedTimeHeaderName, now.toString))

          producer.send(deadQueueProducer.routingKey, rawBody, Some(finalProperties))(cidStrategy)
        })
    }
  }
}

private[rabbitmq] object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"
  final val DiscardedTimeHeaderName: String = "X-Discarded-Time"

  private[rabbitmq] def make[F[_]: Sync: Timer, A](config: Option[PoisonedMessageHandlingConfig],
                                                   connection: RabbitMQConnection[F],
                                                   redactPayload: Boolean,
                                                   monitor: Monitor[F]): Resource[F, PoisonedMessageHandler[F, A]] = {
    config match {
      case Some(LoggingPoisonedMessageHandling(maxAttempts, republishDelay)) =>
        val helper = PoisonedMessageHandlerHelper[F, LoggingPoisonedMessageHandler[F, A]](monitor, redactPayload)
        Resource.pure(new LoggingPoisonedMessageHandler[F, A](maxAttempts, republishDelay, helper))

      case Some(c: DeadQueuePoisonedMessageHandling) =>
        val helper = PoisonedMessageHandlerHelper[F, DeadQueuePoisonedMessageHandler[F, A]](monitor, redactPayload)
        DeadQueuePoisonedMessageHandler.make(c, connection, helper)

      case Some(NoOpPoisonedMessageHandling) | None =>
        Resource.eval {
          val helper = PoisonedMessageHandlerHelper[F, NoOpPoisonedMessageHandler[F, A]](monitor, redactPayload)

          helper.logger.plainWarn("Using NO-OP poisoned message handler. Potential poisoned messages will cycle forever.").as {
            new NoOpPoisonedMessageHandler[F, A](helper)
          }
        }
    }
  }

  private[rabbitmq] def handleResult[F[_]: Sync: Timer, A](
      delivery: Delivery[A],
      rawBody: Bytes,
      messageId: MessageId,
      maxAttempts: Int,
      helper: PoisonedMessageHandlerHelper[F],
      republishDelay: Option[ExponentialDelay],
      handler: PoisonedMessageHandlerAction[F, A])(r: DeliveryResult)(implicit dctx: DeliveryContext): F[DeliveryResult] = {
    r match {
      case Republish(isPoisoned, newHeaders) if isPoisoned =>
        adjustDeliveryResult(delivery, messageId, maxAttempts, newHeaders, helper, republishDelay, handler.handlePoisonedMessage(rawBody))

      case DirectlyPoison => poisonRightAway(delivery, messageId, helper, handler.handlePoisonedMessage(rawBody))

      case r => Applicative[F].pure(r) // keep other results as they are
    }
  }

  private def adjustDeliveryResult[F[_]: Sync: Timer, A](
      delivery: Delivery[A],
      messageId: MessageId,
      maxAttempts: Int,
      newHeaders: Map[String, AnyRef],
      helper: PoisonedMessageHandlerHelper[F],
      republishDelay: Option[ExponentialDelay],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(implicit dctx: DeliveryContext): F[DeliveryResult] = {
    import cats.syntax.traverse._
    import helper._

    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programmatically _pretend_ lower attempt number
    val attempt = (delivery.properties.headers ++ newHeaders)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts for $messageId") >> {
      if (attempt < maxAttempts) {
        for {
          _ <- republishDelay.traverse { d =>
            val delay = d.getExponentialDelay(attempt)
            logger.debug(s"Will republish the message in $delay") >> Timer[F].sleep(delay)
          }
        } yield Republish(countAsPoisoned = true, newHeaders = newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef]))
      } else {
        val now = Instant.now()

        def updateProperties(properties: MessageProperties): MessageProperties = {
          properties.copy(
            headers = properties.headers
              .updated(DiscardedTimeHeaderName, now.toString)
              .updated(RepublishCountHeaderName, maxAttempts.asInstanceOf[AnyRef]))
        }

        val finalDelivery = delivery match {
          case Delivery.Ok(body, properties, routingKey) =>
            Delivery.Ok(body, updateProperties(properties), routingKey)
          case Delivery.MalformedContent(body, properties, routingKey, ce) =>
            Delivery.MalformedContent(body, updateProperties(properties), routingKey, ce)
        }

        handlePoisonedMessage(finalDelivery, maxAttempts)
          .recoverWith {
            case NonFatal(e) => logger.warn(e)("Poisoned message handler failed")
          }
          .map(_ => Reject) // always REJECT the message
      }
    }
  }

  private def poisonRightAway[F[_]: Sync, A](
      delivery: Delivery[A],
      messageId: MessageId,
      helper: PoisonedMessageHandlerHelper[F],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(implicit dctx: DeliveryContext): F[DeliveryResult] = {
    helper.logger.info(s"Directly poisoning delivery $messageId") >>
      handlePoisonedMessage(delivery, 0) >>
      helper.directlyPoisonedMeter.mark >>
      Sync[F].pure(Reject: DeliveryResult)
  }

}

private[rabbitmq] class PoisonedMessageHandlerHelper[F[_]: Sync](val logger: ImplicitContextLogger[F],
                                                                 val monitor: Monitor[F],
                                                                 redactPayload: Boolean) {

  val directlyPoisonedMeter: Meter[F] = monitor.meter("directlyPoisoned")

  def redactIfConfigured(delivery: Delivery[_]): Delivery[Any] = {
    if (!redactPayload) delivery else delivery.withRedactedBody
  }
}

private[rabbitmq] object PoisonedMessageHandlerHelper {
  def apply[F[_]: Sync, PMH: ClassTag](monitor: Monitor[F], redactPayload: Boolean): PoisonedMessageHandlerHelper[F] = {
    val logger: ImplicitContextLogger[F] = ImplicitContextLogger.createLogger[F, PMH]
    new PoisonedMessageHandlerHelper[F](logger, monitor, redactPayload)
  }
}
