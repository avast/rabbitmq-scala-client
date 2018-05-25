package com.avast.clients.rabbitmq.extras

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function.{Function => JavaFunction}

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{FromTask, ToTask, javaapi, _}
import com.avast.utils2.Done
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

class PoisonedMessageHandler[F[_]: FromTask: ToTask, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])
    extends (Delivery[A] => F[DeliveryResult])
    with StrictLogging {

  override def apply(delivery: Delivery[A]): F[DeliveryResult] = convertToF {
    convertFromF {
      wrappedAction(delivery)
    }.flatMap {
      case Republish(newHeaders) => republishDelivery(delivery, newHeaders)
      case r => Task.now(r) // keep other results as they are
    }
  }

  private def republishDelivery(delivery: Delivery[A], newHeaders: Map[String, AnyRef]): Task[DeliveryResult] = {
    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programatically _pretend_ lower attempt number
    val attempt = (newHeaders ++ delivery.properties.headers)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts")

    if (attempt < maxAttempts) {
      Task.now(Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
    } else {
      convertFromF {
        handlePoisonedMessage(delivery)
      }.onErrorRecover {
          case NonFatal(e) =>
            logger.warn("Custom poisoned message handler failed", e)
            Done
        }
        .map(_ => Reject) // always REJECT the message
    }
  }

  /** This method logs the delivery by default but can be overridden. The delivery is always REJECTed after this method execution.
    */
  protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = convertToF {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
    Task.now(())
  }

  private def convertFromF[B](task: F[B]): Task[B] = {
    implicitly[ToTask[F]].apply(task)
  }

  private def convertToF[B](task: Task[B]): F[B] = {
    implicitly[FromTask[F]].apply(task)
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  type JavaAction = JavaFunction[javaapi.Delivery, CompletableFuture[javaapi.DeliveryResult]]
  type CustomJavaPoisonedAction = JavaFunction[javaapi.Delivery, CompletableFuture[Void]]

  def apply[F[_]: FromTask: ToTask, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult]): PoisonedMessageHandler[F, A] = {
    new PoisonedMessageHandler(maxAttempts)(wrappedAction)
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def withCustomPoisonedAction[F[_]: FromTask: ToTask, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])(
      customPoisonedAction: Delivery[A] => F[Unit]): PoisonedMessageHandler[F, A] = {
    new PoisonedMessageHandler(maxAttempts)(wrappedAction) {
      override protected def handlePoisonedMessage(delivery: Delivery[A]) = customPoisonedAction(delivery)
    }
  }

  def forJava(maxAttempts: Int, wrapped: JavaAction, ex: ExecutorService): JavaAction = new JavaAction {
    private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

    private val handler = new PoisonedMessageHandler[Future, Bytes](maxAttempts)(wrapped.asScala)

    override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
      handler(t.asScala).map(_.asJava).asJava
    }
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def forJava(maxAttempts: Int, wrapped: JavaAction, customPoisonedAction: CustomJavaPoisonedAction, ex: ExecutorService): JavaAction =
    new JavaAction {
      private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

      private val handler = new PoisonedMessageHandler[Future, Bytes](maxAttempts)(wrapped.asScala) {
        override protected def handlePoisonedMessage(delivery: Delivery[Bytes]): Future[Unit] = {
          customPoisonedAction(delivery.asJava).asScala.map(_ => ())
        }
      }

      override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
        handler(t.asScala).map(_.asJava).asJava
      }
    }

}
