package com.avast.clients.rabbitmq.extras

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function.{Function => JavaFunction}

import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Republish}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.javaapi
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.utils2.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

class PoisonedMessageHandler(maxCount: Int)(wrappedAction: Delivery => Future[DeliveryResult])(implicit ec: ExecutionContext)
    extends (Delivery => Future[DeliveryResult])
    with StrictLogging {

  override def apply(delivery: Delivery): Future[DeliveryResult] = {
    wrappedAction(delivery).map {
      case Republish(newHeaders) =>
        // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
        // but we're giving the programmer chance to programatically _pretend_ lower attempt number
        val attempt = (newHeaders ++ delivery.properties.headers)
          .get(RepublishCountHeaderName)
          .flatMap(v => Try(v.toString.toInt).toOption)
          .getOrElse(0) + 1

        if (attempt < maxCount) {
          Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef]))
        } else {
          logger.warn(s"Message failures reached the limit $maxCount attempts, throwing away: $delivery")
          Ack
        }

      case r => r // keep other results as they are
    }
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  type JavaAction = JavaFunction[javaapi.Delivery, CompletableFuture[javaapi.DeliveryResult]]

  // for Java users
  def forJava(maxCount: Int, wrapped: JavaAction, ex: Executor): JavaAction = new JavaAction {
    private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

    private val handler = new PoisonedMessageHandler(maxCount)(wrapped.asScala)

    override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
      handler(t.asScala).map(_.asJava).asJava
    }
  }

}
