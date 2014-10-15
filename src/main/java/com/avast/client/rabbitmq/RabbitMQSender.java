package com.avast.client.rabbitmq;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.AMQP;

import java.io.IOException;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQSender {

    /**
     * Sends message to the queue.
     *
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(byte[] msg, AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param msg The message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(byte[] msg) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(ByteString msg, AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param msg The message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(ByteString msg) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(MessageLite msg, AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param msg The message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(MessageLite msg) throws IOException;
}
