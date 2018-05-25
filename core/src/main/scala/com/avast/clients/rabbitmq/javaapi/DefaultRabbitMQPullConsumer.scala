package com.avast.clients.rabbitmq.javaapi

import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{PullResult => ScalaResult, RabbitMQPullConsumer => ScalaConsumer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

class DefaultRabbitMQPullConsumer(scalaConsumer: ScalaConsumer[Future, Bytes] with AutoCloseable)(implicit ec: ExecutionContext)
    extends RabbitMQPullConsumer
    with StrictLogging {
  override def pull(): CompletableFuture[PullResult] = {
    scalaConsumer
      .pull()
      .map[PullResult] {
        case ScalaResult.Ok(dwh) => new PullResult.Ok(dwh.asJava)
        case ScalaResult.EmptyQueue => PullResult.EmptyQueue
      }
      .asJava
  }

  override def close(): Unit = scalaConsumer.close()
}
