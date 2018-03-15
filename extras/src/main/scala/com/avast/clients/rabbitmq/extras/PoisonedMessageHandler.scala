package com.avast.clients.rabbitmq.extras

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function.{Function => JavaFunction}

import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.javaapi
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.utils2.Done
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try
import scala.util.control.NonFatal

class PoisonedMessageHandler(maxCount: Int)(wrappedAction: Delivery => Future[DeliveryResult])(implicit ec: ExecutionContext)
    extends (Delivery => Future[DeliveryResult])
    with StrictLogging {

  override def apply(delivery: Delivery): Future[DeliveryResult] = {
    wrappedAction(delivery).flatMap {
      case Republish(newHeaders) =>
        // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
        // but we're giving the programmer chance to programatically _pretend_ lower attempt number
        val attempt = (newHeaders ++ delivery.properties.headers)
          .get(RepublishCountHeaderName)
          .flatMap(v => Try(v.toString.toInt).toOption)
          .getOrElse(0) + 1

        if (attempt < maxCount) {
          Future.successful(Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
        } else {
          handlePoisonedMessage(delivery)
            .recover {
              case NonFatal(e) =>
                logger.warn("Custom poisoned message handler failed", e)
                Done
            }
            .map(_ => Reject) // always REJECT the message
        }

      case r => Future.successful(r) // keep other results as they are
    }
  }

  /** This method logs the delivery by default but can be overridden. The delivery is always REJECTed after this method execution.
    */
  protected def handlePoisonedMessage(delivery: Delivery): Future[Done] = {
    logger.warn(s"Message failures reached the limit $maxCount attempts, throwing away: $delivery")
    Future.successful(Done)
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  type JavaAction = JavaFunction[javaapi.Delivery, CompletableFuture[javaapi.DeliveryResult]]
  type CustomJavaPoisonedAction = JavaFunction[javaapi.Delivery, CompletableFuture[Void]]

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def withCustomPoisonedAction(maxCount: Int)(wrappedAction: Delivery => Future[DeliveryResult])(
      customPoisonedAction: Delivery => Future[Done])(implicit ec: ExecutionContext): PoisonedMessageHandler = {
    new PoisonedMessageHandler(maxCount)(wrappedAction) {

      override protected def handlePoisonedMessage(delivery: Delivery) = customPoisonedAction(delivery)
    }
  }

  def forJava(maxCount: Int, wrapped: JavaAction, ex: Executor): JavaAction = new JavaAction {
    private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

    private val handler = new PoisonedMessageHandler(maxCount)(wrapped.asScala)

    override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
      handler(t.asScala).map(_.asJava).asJava
    }
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def forJava(maxCount: Int, wrapped: JavaAction, customPoisonedAction: CustomJavaPoisonedAction, ex: Executor): JavaAction =
    new JavaAction {
      private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

      private val handler = new PoisonedMessageHandler(maxCount)(wrapped.asScala) {
        override protected def handlePoisonedMessage(delivery: Delivery): Future[Done] = {
          customPoisonedAction(delivery.asJava).asScala.map(_ => Done)
        }
      }

      override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
        handler(t.asScala).map(_.asJava).asJava
      }
    }

}
