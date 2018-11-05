package com.avast.clients.rabbitmq.api
import scala.language.higherKinds

trait FAutoCloseable[F[_]] {
  def close(): F[Unit]
}
