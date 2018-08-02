package com.avast.clients

import cats.arrow.FunctionK
import cats.~>
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{RecoverableChannel, RecoverableConnection}
import mainecoon.FunctorK
import monix.eval.Task

import scala.language.higherKinds

package object rabbitmq {
  private[rabbitmq] type ServerConnection = RecoverableConnection
  private[rabbitmq] type ServerChannel = RecoverableChannel

  type DeliveryReadAction[F[_], -A] = Delivery[A] => F[DeliveryResult]
  type ParsingFailureAction[F[_]] = (String, Delivery[Bytes], ConversionException) => F[DeliveryResult]

  type FromTask[A[_]] = FunctionK[Task, A]
  type ToTask[A[_]] = FunctionK[A, Task]

  implicit def producerFunctorK[A]: FunctorK[RabbitMQProducer[?[_], A]] = new FunctorK[RabbitMQProducer[?[_], A]] {
    override def mapK[F[_], G[_]](af: RabbitMQProducer[F, A])(fToG: ~>[F, G]): RabbitMQProducer[G, A] =
      (routingKey: String, body: A, properties: Option[MessageProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  private[rabbitmq] implicit class DeliveryOps[A](val d: Delivery[A]) extends AnyVal {
    def mapBody[B](f: A => B): Delivery[B] = d match {
      case ok: Delivery.Ok[A] => ok.copy(body = f(ok.body))
      case m: Delivery.MalformedContent => m
    }

    def flatMap[B](f: Delivery.Ok[A] => Delivery[B]): Delivery[B] = d match {
      case ok: Delivery.Ok[A] => f(ok)
      case m: Delivery.MalformedContent => m
    }
  }

  private[rabbitmq] implicit class DeliveryBytesOps(val d: Delivery[Bytes]) extends AnyVal {

    def toMalformed(ce: ConversionException): Delivery.MalformedContent = d match {
      case ok: Delivery.Ok[Bytes] => Delivery.MalformedContent(ok.body, ok.properties, ok.routingKey, ce)
      case m: Delivery.MalformedContent => m.copy(ce = ce)
    }
  }

}
