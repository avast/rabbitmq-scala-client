package com.avast.clients.rabbitmq.extras

import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.parser._
import scalaj.http.Http

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.util.Success

//noinspection ScalaStyle
class TestHelper(host: String, port: Int) {

  final val RootUri = s"http://$host:$port/api"

  object queue {

    def getMessagesCount(queueName: String): Int = {
      val encoded = URLEncoder.encode(queueName, StandardCharsets.UTF_8.toString)

      val resp = Http(s"$RootUri/queues/%2f/$encoded").auth("guest", "guest").asString.body

      println("MESSAGES COUNT:")
      println(resp)

      decode[QueueProperties](resp) match {
        case Right(p) => p.messages
        case r => throw new IllegalStateException(s"Wrong response $r")
      }
    }

    def getPublishedCount(queueName: String): Int = {
      val encoded = URLEncoder.encode(queueName, StandardCharsets.UTF_8.toString)

      val resp = Http(s"$RootUri/queues/%2f/$encoded").auth("guest", "guest").asString.body

      println("PUBLISHED COUNT:")
      println(resp)

      decode[QueueProperties](resp) match {
        case Right(p) =>
          p.message_stats.map(_.publish).getOrElse {
            Console.err.println(s"Could not extract published_count for $queueName!")
            0
          }
        case r => throw new IllegalStateException(s"Wrong response $r")
      }
    }

    def getArguments(queueName: String): Map[String, Any] = {
      val encoded = URLEncoder.encode(queueName, StandardCharsets.UTF_8.toString)

      val resp = Http(s"$RootUri/queues/%2f/$encoded").auth("guest", "guest").asString.body

      decode[QueueProperties](resp) match {
        case Right(p) =>
          p.arguments.getOrElse {
            Console.err.println(s"Could not extract arguments for $queueName!")
            Map.empty
          }
        case r => throw new IllegalStateException(s"Wrong response $r")
      }
    }

    def delete(queueName: String, ifEmpty: Boolean = false, ifUnused: Boolean = false): Unit = {
      println(s"Deleting queue: $queueName")
      val encoded = URLEncoder.encode(queueName, StandardCharsets.UTF_8.toString)

      val resp =
        Http(s"$RootUri/queues/%2f/$encoded?if-empty=$ifEmpty&if-unused=$ifUnused").method("DELETE").auth("guest", "guest").asString

      val content = resp.body

      val message = s"Delete queue response ${resp.statusLine}: '$content'"
      println(message)

      if (!resp.isSuccess && resp.code != 404) {
        throw new IllegalStateException(message)
      }
    }

    private implicit val anyDecoder: Decoder[Any] = Decoder.decodeJson.emapTry { json =>
      // we are in test, it's enough to support just Int and String here
      if (json.isNumber) {
        json.as[Int].toTry
      } else if (json.isString) {
        json.as[String].toTry
      } else Success(null)
    }

    private case class QueueProperties(messages: Int, message_stats: Option[MessagesStats], arguments: Option[Map[String, Any]])
    private case class MessagesStats(publish: Int, ack: Option[Int])
  }

  object exchange {

    def getPublishedCount(exchangeName: String): Int = {
      val encoded = URLEncoder.encode(exchangeName, StandardCharsets.UTF_8.toString)

      val resp = Http(s"$RootUri/exchanges/%2f/$encoded").auth("guest", "guest").asString.body

      println("PUBLISHED COUNT:")
      println(resp)

      decode[ExchangeProperties](resp) match {
        case Right(p) =>
          p.message_stats.map(_.publish_in).getOrElse {
            Console.err.println(s"Could not extract published_count for $exchangeName!")
            0
          }
        case r => throw new IllegalStateException(s"Wrong response $r")
      }
    }

    private case class ExchangeProperties(message_stats: Option[MessagesStats])
    private case class MessagesStats(publish_in: Int, publish_out: Int)
  }

}
