package com.avast.clients.rabbitmq.javaapi;

import com.avast.bytes.Bytes;

public class Delivery {
    private final Bytes body;
    private final String routingKey;
    private final MessageProperties properties;


    public Delivery(final String routingKey, final Bytes body, final MessageProperties properties) {
        this.routingKey = routingKey;
        this.body = body;
        this.properties = properties;
    }

    public Bytes getBody() {
        return body;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public MessageProperties getProperties() {
        return properties;
    }
}
