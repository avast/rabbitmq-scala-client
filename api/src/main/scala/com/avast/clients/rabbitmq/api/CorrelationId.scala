package com.avast.clients.rabbitmq.api

import cats.effect.Sync
import com.avast.clients.rabbitmq.api.CorrelationId.KeyName

import scala.util.Random

final case class CorrelationId(value: String) extends AnyVal {
  def asContextMap: Map[String, String] = Map(KeyName -> value)
}

object CorrelationId {
  final val KeyName: String = "X-Correlation-Id"

  def create[F[_]: Sync]: F[CorrelationId] = {
    Sync[F].delay {
      createUnsafe
    }
  }

  def createUnsafe: CorrelationId = {
    val id = Random.alphanumeric.take(20).mkString
    CorrelationId(id)
  }
}
