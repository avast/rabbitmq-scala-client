package com.avast.clients.rabbitmq

import cats.Applicative
import cats.effect.{Resource, Sync, Timer}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFunctorOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.PoisonedMessageHandler.{defaultHandlePoisonedMessage, DiscardedTimeHeaderName}
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor

import java.time.Instant
import scala.util.Try
import scala.util.control.NonFatal

sealed trait PoisonedMessageHandler[F[_], A] {
  def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult]
}

class LoggingPoisonedMessageHandler[F[_]: Sync: Timer, A](maxAttempts: Int, republishDelay: Option[ExponentialDelay])
    extends PoisonedMessageHandler[F, A] {
  private val logger = ImplicitContextLogger.createLogger[F, LoggingPoisonedMessageHandler[F, A]]

  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult] = {
    PoisonedMessageHandler.handleResult(delivery,
                                        messageId,
                                        maxAttempts,
                                        logger,
                                        republishDelay,
                                        (d: Delivery[A], _) => defaultHandlePoisonedMessage[F, A](maxAttempts, logger)(d))(result)
  }
}

class NoOpPoisonedMessageHandler[F[_]: Sync, A] extends PoisonedMessageHandler[F, A] {
  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult] = Sync[F].pure(result)
}

class DeadQueuePoisonedMessageHandler[F[_]: Sync: Timer, A](maxAttempts: Int, republishDelay: Option[ExponentialDelay])(
    moveToDeadQueue: (Delivery[A], Bytes, DeliveryContext) => F[Unit])
    extends PoisonedMessageHandler[F, A] {
  private val logger = ImplicitContextLogger.createLogger[F, DeadQueuePoisonedMessageHandler[F, A]]

  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit dctx: DeliveryContext): F[DeliveryResult] = {
    PoisonedMessageHandler.handleResult(delivery,
                                        messageId,
                                        maxAttempts,
                                        logger,
                                        republishDelay,
                                        (d, _) => handlePoisonedMessage(d, messageId, rawBody))(result)
  }

  private def handlePoisonedMessage(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(
      implicit dctx: DeliveryContext): F[Unit] = {

    logger.warn {
      s"Message $messageId failures reached the limit $maxAttempts attempts, moving it to the dead queue: $delivery"
    } >>
      moveToDeadQueue(delivery, rawBody, dctx) >>
      logger.debug(s"Message $messageId moved to the dead queue")
  }
}

object DeadQueuePoisonedMessageHandler {
  def make[F[_]: Sync: Timer, A](c: DeadQueuePoisonedMessageHandling,
                                 connection: RabbitMQConnection[F],
                                 monitor: Monitor[F]): Resource[F, DeadQueuePoisonedMessageHandler[F, A]] = {
    val dqpc = c.deadQueueProducer
    val pc = ProducerConfig(
      name = dqpc.name,
      exchange = dqpc.exchange,
      declare = dqpc.declare,
      reportUnroutable = dqpc.reportUnroutable,
      sizeLimitBytes = dqpc.sizeLimitBytes,
      properties = dqpc.properties
    )

    connection.newProducer[Bytes](pc, monitor.named("deadQueueProducer")).map { producer =>
      new DeadQueuePoisonedMessageHandler[F, A](c.maxAttempts, c.republishDelay)((d: Delivery[A], rawBody: Bytes, dctx: DeliveryContext) => {
          val cidStrategy = dctx.correlationId match {
            case Some(value) => CorrelationIdStrategy.Fixed(value.value)
            case None => CorrelationIdStrategy.RandomNew
          }

          val now = Instant.now()

        val finalProperties = d.properties.copy(headers = d.properties.headers.updated(DiscardedTimeHeaderName, now.toString))

        producer.send(dqpc.routingKey, rawBody, Some(finalProperties))(cidStrategy)
        })
    }
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"
  final val DiscardedTimeHeaderName: String = "X-Discarded-Time"

  private[rabbitmq] def make[F[_]: Sync: Timer, A](config: Option[PoisonedMessageHandlingConfig],
                                                   connection: RabbitMQConnection[F],
                                                   monitor: Monitor[F]): Resource[F, PoisonedMessageHandler[F, A]] = {
    config match {
      case Some(LoggingPoisonedMessageHandling(maxAttempts, republishDelay)) =>
        Resource.pure(new LoggingPoisonedMessageHandler[F, A](maxAttempts, republishDelay))
      case Some(c: DeadQueuePoisonedMessageHandling) => DeadQueuePoisonedMessageHandler.make(c, connection, monitor)
      case Some(NoOpPoisonedMessageHandling) | None =>
        Resource.eval {
          val logger = ImplicitContextLogger.createLogger[F, NoOpPoisonedMessageHandler[F, A]]
          logger.plainWarn("Using NO-OP poisoned message handler. Potential poisoned messages will cycle forever.").as {
            new NoOpPoisonedMessageHandler[F, A]
          }
        }
    }
  }

  private[rabbitmq] def defaultHandlePoisonedMessage[F[_]: Sync, A](maxAttempts: Int, logger: ImplicitContextLogger[F])(
      delivery: Delivery[A])(implicit dctx: DeliveryContext): F[Unit] = {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
  }

  private[rabbitmq] def handleResult[F[_]: Sync: Timer, A](
      delivery: Delivery[A],
      messageId: MessageId,
      maxAttempts: Int,
      logger: ImplicitContextLogger[F],
      republishDelay: Option[ExponentialDelay],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(r: DeliveryResult)(implicit dctx: DeliveryContext): F[DeliveryResult] = {
    r match {
      case Republish(isPoisoned, newHeaders) if isPoisoned =>
        adjustDeliveryResult(delivery, messageId, maxAttempts, newHeaders, logger, republishDelay, handlePoisonedMessage)
      case r => Applicative[F].pure(r) // keep other results as they are
    }
  }

  private def adjustDeliveryResult[F[_]: Sync: Timer, A](
      delivery: Delivery[A],
      messageId: MessageId,
      maxAttempts: Int,
      newHeaders: Map[String, AnyRef],
      logger: ImplicitContextLogger[F],
      republishDelay: Option[ExponentialDelay],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(implicit dctx: DeliveryContext): F[DeliveryResult] = {
    import cats.syntax.traverse._

    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programmatically _pretend_ lower attempt number
    val attempt = (delivery.properties.headers ++ newHeaders)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts for $messageId") >> {
      if (attempt < maxAttempts) {
        for {
          _ <- republishDelay.traverse(d => Timer[F].sleep(d.getExponentialDelay(attempt)))
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
            case NonFatal(e) =>
              logger.warn(e)("Custom poisoned message handler failed")
          }
          .map(_ => Reject) // always REJECT the message
      }
    }
  }

}
