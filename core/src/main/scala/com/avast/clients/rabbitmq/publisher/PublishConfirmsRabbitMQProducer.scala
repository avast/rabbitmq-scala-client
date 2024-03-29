package com.avast.clients.rabbitmq.publisher

import cats.effect.Concurrent.ops.toAllConcurrentOps
import cats.effect.concurrent.Deferred
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MaxAttemptsReached, MessageProperties, NotAcknowledgedPublish}
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.clients.rabbitmq.{CorrelationId, ProductConverter, ServerChannel, startAndForget}
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.ConfirmListener

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
class PublishConfirmsRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                                 exchangeName: String,
                                                                 channel: ServerChannel,
                                                                 defaultProperties: MessageProperties,
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

  private[rabbitmq] val confirmationCallbacks = {
    new ConcurrentHashMap[Long, Deferred[F, Either[NotAcknowledgedPublish, Unit]]]().asScala
  }

  override def sendMessage(routingKey: String, body: Bytes, properties: MessageProperties)(implicit correlationId: CorrelationId): F[Unit] =
    sendWithAck(routingKey, body, properties, 1)

  private def sendWithAck(routingKey: String, body: Bytes, properties: MessageProperties, attemptCount: Int)(
      implicit correlationId: CorrelationId): F[Unit] = {

    if (attemptCount > sendAttempts) {
      F.raiseError(MaxAttemptsReached("Exhausted max number of attempts"))
    } else {
      for {
        confirmationCallback <- Deferred.apply[F, Either[NotAcknowledgedPublish, Unit]]
        sequenceNumber <- basicSend(routingKey, body, properties, (sequenceNumber: Long) => {
          confirmationCallbacks += sequenceNumber -> confirmationCallback
        })
        result <- confirmationCallback.get
        _ <- F.delay(confirmationCallbacks -= sequenceNumber)
        _ <- result match {
          case Left(err) =>
            val sendResult = if (sendAttempts > 1) {
              logger.plainTrace(s"Republishing nacked message with sequenceNumber $sequenceNumber") >>
                sendWithAck(routingKey, body, properties, attemptCount + 1)
            } else {
              F.raiseError(err)
            }
            nacked.mark >> sendResult
          case Right(_) =>
            acked.mark
        }
      } yield ()
    }
  }

  private[rabbitmq] object DefaultConfirmListener extends ConfirmListener {

    override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Acked $deliveryTag, multiple: $multiple") >> completeDefer(deliveryTag, multiple, Right(()))
      }
    }

    override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Nacked $deliveryTag, multiple: $multiple") >> completeDefer(
          deliveryTag,
          multiple,
          Left(NotAcknowledgedPublish(s"Broker was unable to process the message", messageId = deliveryTag)))
      }
    }

    private def completeDefer(deliveryTag: Long, multiple: Boolean, result: Either[NotAcknowledgedPublish, Unit]): F[Unit] = {
      if (multiple) {
        confirmationCallbacks
          .filter {
            case (sequenceNumber, _) => sequenceNumber <= deliveryTag
          }
          .values
          .toList
          .traverse { callback =>
            callback.complete(result).start
          }
          .void
      } else {
        confirmationCallbacks.get(deliveryTag) match {
          case Some(callback) => callback.complete(result)
          case None =>
            logger.plainDebug(s"Received confirmation for unknown delivery tag $deliveryTag with result $result.")
        }
      }
    }
  }
}
