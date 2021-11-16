package com.avast.clients.rabbitmq.api

import scala.annotation.implicitNotFound
import scala.util.Random

@implicitNotFound("You have to specify the CorrelationIdStrategy to proceed")
trait CorrelationIdStrategy {
  def toCIDValue: String
}

object CorrelationIdStrategy {
  final val CorrelationIdKeyName: String = "X-Correlation-Id"

  /**
    * Generate a new, random CorrelationId.
    *
    * Example:
    * {{{
    * implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.RandomNew
    * }}}
    */
  case object RandomNew extends CorrelationIdStrategy {
    override def toCIDValue: String = randomValue
  }

  /**
    * Always provide same value.
    *
    * Example:
    * {{{
    * implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.Fixed("my-corr-id")
    * }}}
    */
  case class Fixed(value: String) extends CorrelationIdStrategy {
    override val toCIDValue: String = value
  }

  /**
    * Try to find the CID in properties (or `X-Correlation-Id` header as a fallback). Generate a new, random one, if nothing was found.
    *
    * Example:
    * {{{
    * val mp = MessageProperties(correlationId = Some(cid))
    * ...
    * implicit val cidStrategy: CorrelationIdStrategy = CorrelationIdStrategy.FromPropertiesOrRandomNew(Some(mp))
    * }}}
    */
  case class FromPropertiesOrRandomNew(mp: Option[MessageProperties]) extends CorrelationIdStrategy {
    override lazy val toCIDValue: String = {
      // take it from properties or from header (as a fallback)... if still empty, generate new
      mp.flatMap(p => p.correlationId.orElse(p.headers.get(CorrelationIdKeyName).map(_.toString))).getOrElse(randomValue)
    }
  }

  private def randomValue: String = {
    Random.alphanumeric.take(20).mkString
  }
}
