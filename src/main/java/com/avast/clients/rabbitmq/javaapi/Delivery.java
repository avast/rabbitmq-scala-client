package com.avast.clients.rabbitmq.javaapi;

import com.avast.bytes.Bytes;
import com.rabbitmq.client.BasicProperties;

public class Delivery {
    private final com.avast.clients.rabbitmq.Delivery scalaDelivery;

    public Delivery(com.avast.clients.rabbitmq.Delivery scalaDelivery) {
        this.scalaDelivery = scalaDelivery;
    }

    public Bytes getBody() {
        return scalaDelivery.body();
    }

    public BasicProperties getProperties() {
        return scalaDelivery.properties();
    }

    public String getRoutingKey() {
        return scalaDelivery.routingKey();
    }
}
