package com.avast.clients.rabbitmq.api

case class Delivery[A](body: A, properties: MessageProperties, routingKey: String)
