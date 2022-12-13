package com.avast.clients.rabbitmq.publisher

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MaxAttemptsReached, MessageProperties, NotAcknowledgedPublish}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.publisher.PublishConfirmsRabbitMQProducer.SentMessages
import com.avast.clients.rabbitmq.{CorrelationId, ProductConverter, ServerChannel, startAndForget}
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.ConfirmListener

class PublishConfirmsRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                                 exchangeName: String,
                                                                 channel: ServerChannel,
                                                                 defaultProperties: MessageProperties,
                                                                 sentMessages: SentMessages[F],
                                                                 sendAttempts: Int,
                                                                 reportUnroutable: Boolean,
                                                                 sizeLimitBytes: Option[Int],
                                                                 blocker: Blocker,
                                                                 logger: ImplicitContextLogger[F],
                                                                 monitor: Monitor[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F])
    extends BaseRabbitMQProducer[F, A](name,
                                       exchangeName,
                                       channel,
                                       defaultProperties,
                                       reportUnroutable,
                                       sizeLimitBytes,
                                       blocker,
                                       logger,
                                       monitor) {

  channel.confirmSelect()
  channel.addConfirmListener(DefaultConfirmListener)

  private val acked = monitor.meter("acked")
  private val nacked = monitor.meter("nacked")

  override def sendMessage(routingKey: String, body: Bytes, properties: MessageProperties)(implicit correlationId: CorrelationId): F[Unit] =
    sendWithAck(routingKey, body, properties, 1)

  private def sendWithAck(routingKey: String, body: Bytes, properties: MessageProperties, attemptCount: Int)(
      implicit correlationId: CorrelationId): F[Unit] = {

    if (attemptCount > sendAttempts) {
      F.raiseError(MaxAttemptsReached("Exhausted max number of attempts"))
    } else {
      val messageId = channel.getNextPublishSeqNo
      for {
        defer <- Deferred.apply[F, Either[NotAcknowledgedPublish, Unit]]
        _ <- sentMessages.update(_ + (messageId -> defer))
        _ <- basicSend(routingKey, body, properties)
        result <- defer.get
        _ <- result match {
          case Left(err) =>
            val sendResult = if (sendAttempts > 1) {
              clearProcessedMessage(messageId) >> sendWithAck(routingKey, body, properties, attemptCount + 1)
            } else {
              F.raiseError(err)
            }

            nacked.mark >> sendResult
          case Right(_) =>
            acked.mark >> clearProcessedMessage(messageId)
        }
      } yield ()
    }
  }

  private def clearProcessedMessage(messageId: Long): F[Unit] = {
    sentMessages.update(_ - messageId)
  }

  private object DefaultConfirmListener extends ConfirmListener {
    import cats.syntax.foldable._

    override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Acked $deliveryTag") >> completeDefer(deliveryTag, Right(()))
      }
    }

    override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Not acked $deliveryTag") >> completeDefer(
          deliveryTag,
          Left(NotAcknowledgedPublish(s"Message $deliveryTag not acknowledged by broker", messageId = deliveryTag)))
      }
    }

    private def completeDefer(deliveryTag: Long, result: Either[NotAcknowledgedPublish, Unit]): F[Unit] = {
      sentMessages.get.flatMap(_.get(deliveryTag).traverse_(_.complete(result)))
    }
  }

}

object PublishConfirmsRabbitMQProducer {
  type SentMessages[F[_]] = Ref[F, Map[Long, Deferred[F, Either[NotAcknowledgedPublish, Unit]]]]
}
