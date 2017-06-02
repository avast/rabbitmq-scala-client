package com.avast.clients.rabbitmq

import java.util.concurrent.CompletableFuture

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.HealthCheckStatus.{Failure, Ok}
import com.avast.yap.api.http.{HttpRequest, HttpResponse}
import com.avast.yap.server.http.HttpResponses
import com.typesafe.scalalogging.StrictLogging

class YapHealthCheck extends HealthCheck with (HttpRequest[Bytes] => CompletableFuture[HttpResponse[Bytes]]) with StrictLogging {

  override def apply(req: HttpRequest[Bytes]): CompletableFuture[HttpResponse[Bytes]] = {
    val resp = getStatus match {
      case Ok => HttpResponses.Ok()
      case Failure(msg, _) => HttpResponses.InternalServerError(Bytes.copyFromUtf8(msg))
    }
    CompletableFuture.completedFuture(resp)
  }

}
