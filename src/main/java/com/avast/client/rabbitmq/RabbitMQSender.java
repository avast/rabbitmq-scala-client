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
    void send(byte[] msg, AMQP.BasicProperties properties) throws IOException;

    void send(byte[] msg) throws IOException;

    void send(ByteString msg, AMQP.BasicProperties properties) throws IOException;

    void send(ByteString msg) throws IOException;

    void send(MessageLite msg, AMQP.BasicProperties properties) throws IOException;

    void send(MessageLite msg) throws IOException;
}
