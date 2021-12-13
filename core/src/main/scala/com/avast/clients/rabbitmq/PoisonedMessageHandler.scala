package com.avast.clients.rabbitmq

import cats.Applicative
import cats.effect.{Resource, Sync}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFunctorOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.PoisonedMessageHandler.defaultHandlePoisonedMessage
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor

import scala.util.Try
import scala.util.control.NonFatal

sealed trait PoisonedMessageHandler[F[_], A] {
  def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit correlationId: CorrelationId): F[DeliveryResult]
}

class LoggingPoisonedMessageHandler[F[_]: Sync, A](maxAttempts: Int) extends PoisonedMessageHandler[F, A] {
  private val logger = ImplicitContextLogger.createLogger[F, LoggingPoisonedMessageHandler[F, A]]

  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit correlationId: CorrelationId): F[DeliveryResult] = {
    PoisonedMessageHandler.handleResult(delivery,
                                        messageId,
                                        maxAttempts,
                                        logger,
                                        (d: Delivery[A], _) => defaultHandlePoisonedMessage[F, A](maxAttempts, logger)(d))(result)
  }
}

class NoOpPoisonedMessageHandler[F[_]: Sync, A] extends PoisonedMessageHandler[F, A] {
  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit correlationId: CorrelationId): F[DeliveryResult] = Sync[F].pure(result)
}

class DeadQueuePoisonedMessageHandler[F[_]: Sync, A](maxAttempts: Int)(moveToDeadQueue: (Delivery[A], Bytes, CorrelationId) => F[Unit])
    extends PoisonedMessageHandler[F, A] {
  private val logger = ImplicitContextLogger.createLogger[F, DeadQueuePoisonedMessageHandler[F, A]]

  override def interceptResult(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(result: DeliveryResult)(
      implicit correlationId: CorrelationId): F[DeliveryResult] = {
    PoisonedMessageHandler.handleResult(delivery, messageId, maxAttempts, logger, (d, _) => handlePoisonedMessage(d, messageId, rawBody))(
      result)
  }

  private def handlePoisonedMessage(delivery: Delivery[A], messageId: MessageId, rawBody: Bytes)(
      implicit correlationId: CorrelationId): F[Unit] = {

    logger.warn {
      s"Message $messageId failures reached the limit $maxAttempts attempts, moving it to the dead queue: $delivery"
    } >>
      moveToDeadQueue(delivery, rawBody, correlationId) >>
      logger.debug(s"Message $messageId moved to the dead queue")
  }
}

object DeadQueuePoisonedMessageHandler {
  def make[F[_]: Sync, A](c: DeadQueuePoisonedMessageHandling,
                          connection: RabbitMQConnection[F],
                          monitor: Monitor): Resource[F, DeadQueuePoisonedMessageHandler[F, A]] = {
    val dqpc = c.deadQueueProducer
    val pc = ProducerConfig(name = dqpc.name,
                            exchange = dqpc.exchange,
                            declare = dqpc.declare,
                            reportUnroutable = dqpc.reportUnroutable,
                            properties = dqpc.properties)

    connection.newProducer[Bytes](pc, monitor.named("deadQueueProducer")).map { producer =>
      new DeadQueuePoisonedMessageHandler[F, A](c.maxAttempts)({ (d: Delivery[A], rawBody: Bytes, correlationId: CorrelationId) =>
        producer.send(dqpc.routingKey, rawBody, Some(d.properties))(correlationId)
      })
    }
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  private[rabbitmq] def make[F[_]: Sync, A](config: Option[PoisonedMessageHandlingConfig],
                                            connection: RabbitMQConnection[F],
                                            monitor: Monitor): Resource[F, PoisonedMessageHandler[F, A]] = {
    config match {
      case Some(LoggingPoisonedMessageHandling(maxAttempts)) => Resource.pure(new LoggingPoisonedMessageHandler[F, A](maxAttempts))
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
      delivery: Delivery[A])(implicit correlationId: CorrelationId): F[Unit] = {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
  }

  private[rabbitmq] def handleResult[F[_]: Sync, A](
      delivery: Delivery[A],
      messageId: MessageId,
      maxAttempts: Int,
      logger: ImplicitContextLogger[F],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(r: DeliveryResult)(implicit correlationId: CorrelationId): F[DeliveryResult] = {
    r match {
      case Republish(isPoisoned, newHeaders) if isPoisoned =>
        adjustDeliveryResult(delivery, messageId, maxAttempts, newHeaders, logger, handlePoisonedMessage)
      case r => Applicative[F].pure(r) // keep other results as they are
    }
  }

  private def adjustDeliveryResult[F[_]: Sync, A](
      delivery: Delivery[A],
      messageId: MessageId,
      maxAttempts: Int,
      newHeaders: Map[String, AnyRef],
      logger: ImplicitContextLogger[F],
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(implicit correlationId: CorrelationId): F[DeliveryResult] = {
    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programmatically _pretend_ lower attempt number
    val attempt = (delivery.properties.headers ++ newHeaders)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts for $messageId") >> {
      if (attempt < maxAttempts) {
        Applicative[F].pure(
          Republish(isPoisoned = true, newHeaders = newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
      } else {
        handlePoisonedMessage(delivery, maxAttempts)
          .recoverWith {
            case NonFatal(e) =>
              logger.warn(e)("Custom poisoned message handler failed")
          }
          .map(_ => Reject) // always REJECT the message
      }
    }
  }

}
