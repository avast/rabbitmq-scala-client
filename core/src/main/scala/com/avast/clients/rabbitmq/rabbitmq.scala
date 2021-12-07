package com.avast.clients

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits.catsSyntaxFlatMapOps
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer.{FederationOriginalRoutingKeyHeaderName, RepublishOriginalRoutingKeyHeaderName}
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{RecoverableChannel, RecoverableConnection}
import fs2.RaiseThrowable

import java.util.concurrent.Executors
import scala.language.implicitConversions

package object rabbitmq {
  private[rabbitmq] type ServerConnection = RecoverableConnection
  private[rabbitmq] type ServerChannel = RecoverableChannel

  private[rabbitmq] val ses = Executors.newScheduledThreadPool(2)

  type DeliveryReadAction[F[_], -A] = Delivery[A] => F[DeliveryResult]
  type ParsingFailureAction[F[_]] = (String, Delivery[Bytes], ConversionException) => F[DeliveryResult]

  implicit class StreamOps[F[_], A](val stream: fs2.Stream[F, A]) {
    def makeResilient(maxErrors: Int)(logError: Throwable => F[Unit])(implicit F: Sync[F], rt: RaiseThrowable[F]): fs2.Stream[F, A] = {
      fs2.Stream.eval(Ref[F].of(maxErrors)).flatMap { failureCounter =>
        lazy val resilientStream: fs2.Stream[F, A] = stream.handleErrorWith { err =>
          fs2.Stream.eval(logError(err) >> failureCounter.modify(a => (a - 1, a - 1))).flatMap { attemptsRest =>
            if (attemptsRest <= 0) fs2.Stream.raiseError[F](err) else resilientStream
          }
        }

        resilientStream
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

  private[rabbitmq] implicit class BasicPropertiesOps(val props: com.rabbitmq.client.AMQP.BasicProperties) extends AnyVal {
    def getOriginalRoutingKey: Option[String] = {
      for {
        headers <- Option(props.getHeaders)
        ork <- {
          Option(headers.get(RepublishOriginalRoutingKeyHeaderName))
            .orElse(Option(headers.get(FederationOriginalRoutingKeyHeaderName)))
        }
      } yield ork.toString
    }
  }

}
