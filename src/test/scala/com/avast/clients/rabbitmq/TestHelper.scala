package com.avast.clients.rabbitmq

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import cats.data.Xor._
import io.circe.generic.auto._
import io.circe.parser._

import scalaj.http.Http

class TestHelper(host: String, port: Int) {

  final val RootUri = s"http://$host:$port/api"

  def getMessagesCount(queueName: String): Int = {
    val encoded = URLEncoder.encode(queueName, StandardCharsets.UTF_8.toString)

    val resp = Http(s"$RootUri/queues/%2f/$encoded").auth("guest", "guest").asString.body

    decode[QueueProperties](resp) match {
      case Right(p) =>
        p.messages
      case r => throw new IllegalStateException(s"Wrong response $r")
    }
  }

  private case class QueueProperties(messages: Int)

}
