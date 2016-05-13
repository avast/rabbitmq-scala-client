package com.avast.clients.rabbitmq.api

sealed trait Result

object Result {

  case object Ok extends Result

  case object Failed extends Result

}
