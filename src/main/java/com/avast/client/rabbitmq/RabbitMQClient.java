package com.avast.client.rabbitmq;

import com.avast.jmx.JMXProperty;

import java.io.Closeable;

/**
 * Created <b>4.12.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQClient extends Closeable {
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
