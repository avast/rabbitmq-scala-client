package com.avast.client.rabbitmq;

import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

import java.io.Closeable;

/**
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQClient extends Closeable {

    /**
     * Gets currently connected channel.
     *
     * @return The cnannel.
     */
    public AutorecoveringChannel getChannel();

    /**
     * Closes this client quietly, only logs errors.
     */
    void closeQuietly();

    /**
     * Queries whether this client was closed.
     *
     * @return TRUE if it was closed.
     */
    boolean isClosed();

    /**
     * Queries whether this client is alive; it means it wasn't closed and is connected to the server.
     *
     * @return TRUE if this client is connected to the server. Returns FALSE e.g. in case of connection failure.
     */
    boolean isAlive();
}
