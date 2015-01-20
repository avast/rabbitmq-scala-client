package com.avast.client.rabbitmq;

import com.rabbitmq.client.Channel;

import java.io.IOException;

/**
 * Adds option to declare queue, binding or exchange before start of consuming or listening
 *
 * Created by tunkl@avast.com on 20.01.15.
 */
public interface RabbitMQDeclare {

    /**
     * Is called after channgel creation, before start of consuming or listening
     * @param channel
     */
    public void declare(Channel channel) throws IOException;
}
