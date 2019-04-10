package com.avast.clients.rabbitmq.extras

import java.util.concurrent.{CompletableFuture, ExecutorService}
import java.util.function.{Function => JavaFunction}

import cats.MonadError
import cats.implicits._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.clients.rabbitmq.{javaapi, _}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

trait PoisonedMessageHandler[F[_], A] extends (Delivery[A] => F[DeliveryResult])

private[rabbitmq] class DefaultPoisonedMessageHandler[F[_]: FailableMonad, A](maxAttempts: Int)(
    wrappedAction: Delivery[A] => F[DeliveryResult])
    extends PoisonedMessageHandler[F, A]
    with StrictLogging {

  private val F = implicitly[FailableMonad[F]]

  override def apply(delivery: Delivery[A]): F[DeliveryResult] = {
    wrappedAction(delivery)
      .flatMap {
        case Republish(newHeaders) => republishDelivery(delivery, newHeaders)
        case r => F.pure(r) // keep other results as they are
      }
  }

  private def republishDelivery(delivery: Delivery[A], newHeaders: Map[String, AnyRef]): F[DeliveryResult] = {
    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programatically _pretend_ lower attempt number
    val attempt = (newHeaders ++ delivery.properties.headers)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts")

    if (attempt < maxAttempts) {
      F.pure(Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
    } else {
      handlePoisonedMessage(delivery)
        .recover {
          case NonFatal(e) =>
            logger.warn("Custom poisoned message handler failed", e)
            ()
        }
        .map(_ => Reject) // always REJECT the message
    }
  }

  /** This method logs the delivery by default but can be overridden. The delivery is always REJECTed after this method execution.
    */
  protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
    F.unit
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  type FailableMonad[F[_]] = MonadError[F, Throwable]

  type JavaAction = JavaFunction[javaapi.Delivery, CompletableFuture[javaapi.DeliveryResult]]
  type CustomJavaPoisonedAction = JavaFunction[javaapi.Delivery, CompletableFuture[Void]]

  def apply[F[_]: FailableMonad, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult]): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts)(wrappedAction)
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def withCustomPoisonedAction[F[_]: FailableMonad, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])(
      customPoisonedAction: Delivery[A] => F[Unit]): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts)(wrappedAction) {
      override protected def handlePoisonedMessage(delivery: Delivery[A]) = customPoisonedAction(delivery)
    }
  }

  def forJava(maxAttempts: Int, wrapped: JavaAction, ex: ExecutorService): JavaAction = new JavaAction {
    private implicit val sch: Scheduler = Scheduler(ses, ExecutionContext.fromExecutor(ex))

    private val handler = new DefaultPoisonedMessageHandler[Task, Bytes](maxAttempts)(d => Task.deferFuture(wrapped.asScala.apply(d)))

    override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
      handler(t.asScala).map(_.asJava).runToFuture.asJava
    }
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def forJava(maxAttempts: Int, wrapped: JavaAction, customPoisonedAction: CustomJavaPoisonedAction, ex: ExecutorService): JavaAction =
    new JavaAction {
      private implicit val sch: Scheduler = Scheduler(ses, ExecutionContext.fromExecutor(ex))

      private val handler = new DefaultPoisonedMessageHandler[Task, Bytes](maxAttempts)(d => Task.deferFuture(wrapped.asScala.apply(d))) {
        override protected def handlePoisonedMessage(delivery: Delivery[Bytes]): Task[Unit] = {
          Task.deferFuture {
            customPoisonedAction(delivery.asJava).asScala.map(_ => ())
          }
        }
      }

      override def apply(t: javaapi.Delivery): CompletableFuture[javaapi.DeliveryResult] = {
        handler(t.asScala).map(_.asJava).runToFuture.asJava
      }
    }

}
