package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.avast.metrics.api.Meter;
import com.avast.metrics.api.Monitor;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;

import javax.annotation.concurrent.ThreadSafe;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Date;

/**
 * @author Jenda Kolena, kolena@avast.com
 */
@SuppressWarnings("unused")
@ThreadSafe
public class DefaultRabbitMQSender extends RabbitMQClientBase implements RabbitMQSender {
    public static final String EXCHANGE_DEFAULT = "";
    public static final String ROUTING_KEY_DEFAULT = null;

    protected final Meter sentMeter;
    protected final Meter failedMeter;

    public DefaultRabbitMQSender(final Address[] addresses, final String virtualHost, final String username, final String password, final int connectionTimeout, final int recoveryTimeout, final SSLContext sslContext, final ExceptionHandler exceptionHandler, final RabbitMQDeclare declare, final Monitor metricsMonitor, final String name) throws RequestConnectException {
        super("SENDER", addresses, virtualHost, username, password, connectionTimeout, recoveryTimeout, sslContext, exceptionHandler, metricsMonitor, name);

        try {
            startSender(declare);
        } catch (IOException e) {
            throw new RuntimeException("Error during starting sender.", e);
        }

        sentMeter = metricsMonitor.newMeter("sent");
        failedMeter = metricsMonitor.newMeter("failed");
    }

    @Override
    protected void onChannelRecovered(Recoverable recoverable) {
        try {
            startSender(null);
        } catch (IOException e) {
            LOG.error("Error while restarting the sender", e);
        }
    }

    protected synchronized void startSender(RabbitMQDeclare declare) throws IOException {
        if (declare != null) {
            declare.declare(getChannel());
        }
    }

    @Override
    public synchronized void send(final String exchange, final String routingKey, final byte[] msg, final AMQP.BasicProperties properties) throws IOException {
        LOG.debug("Sending message with length " + (msg != null ? msg.length : 0) + " to " + connection.getAddress().getHostName());
        try {
            channel.basicPublish(exchange, routingKey, properties, msg);
            sentMeter.mark();
        } catch (IOException e) {
            failedMeter.mark();
            LOG.debug("Error while sending the message", e);
            throw e;
        }
    }

    @Override
    public void send(final String exchange, final byte[] msg, final AMQP.BasicProperties properties) throws IOException {
        send(exchange, ROUTING_KEY_DEFAULT, msg, properties);
    }

    @Override
    public void send(final String exchange, final byte[] msg) throws IOException {
        send(exchange, ROUTING_KEY_DEFAULT, msg, createProperties());
    }

    @Override
    public void send(final String exchange, final String routingKey, final MessageLite msg, final AMQP.BasicProperties properties) throws IOException {
        send(exchange, ROUTING_KEY_DEFAULT, msg.toByteArray(), properties);
    }

    @Override
    public void send(final String exchange, final MessageLite msg, final AMQP.BasicProperties properties) throws IOException {
        send(exchange, ROUTING_KEY_DEFAULT, msg, properties);
    }

    @Override
    public void send(final String exchange, final MessageLite msg) throws IOException {
        send(exchange, ROUTING_KEY_DEFAULT, msg, createProperties());
    }

    public static AMQP.BasicProperties createProperties(String msgType, String contentType, String expiration) {
        return new AMQP.BasicProperties.Builder()
                .expiration(expiration)
                .contentType(contentType)
                .contentEncoding("utf-8")
                .deliveryMode(2)
                .correlationId(null)
                .priority(0)
                .type(msgType)
                .timestamp(new Date())
                .build();
    }

    public static AMQP.BasicProperties createProperties(String msgType, String contentType) {
        return createProperties(msgType, contentType, null);
    }

    public static AMQP.BasicProperties createProperties(String msgType) {
        return createProperties(msgType, "application/octet-stream", null);
    }

    public static AMQP.BasicProperties createProperties() {
        return createProperties(null, "application/octet-stream", null);
    }
}
