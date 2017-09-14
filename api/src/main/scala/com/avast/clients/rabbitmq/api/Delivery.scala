package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes

case class Delivery(body: Bytes, properties: MessageProperties, routingKey: String)
