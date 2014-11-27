package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
@SuppressWarnings("unused")
public class DefaultRabbitMQSender implements RabbitMQSender {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("mqsender-%d").build());

    protected final String host;
    protected final String queue;
    protected final Channel channel;

    public DefaultRabbitMQSender(final String host, final String username, final String password, final String queue, final int connectionTimeout, final boolean useSSL) throws RequestConnectException {
        this.host = host;
        this.queue = queue;

        try {
            final ConnectionFactory factory = new ConnectionFactory();
            if (host.contains("/")) {
                final String[] parts = host.split("/");
                if (parts.length > 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
                    throw new IllegalArgumentException("Invalid definition of host/virtualhost");
                }
                factory.setHost(parts[0]);
                factory.setVirtualHost(parts[1]);
            } else {
                factory.setHost(host);
            }

            if (useSSL) factory.useSslProtocol();
            factory.setSharedExecutor(executor);
            factory.setExceptionHandler(getExceptionHandler());
            factory.setConnectionTimeout(connectionTimeout > 0 ? connectionTimeout : 5000);
            if (StringUtils.isNotBlank(username)) {
                factory.setUsername(username);
            }
            if (StringUtils.isNotBlank(password)) {
                factory.setPassword(password);
            }

            LOG.info("Connecting to RabbitMQ on " + host + "/" + queue);
            channel = factory.newConnection().createChannel();
            LOG.debug("Connected to " + host + "/" + queue);

            channel.queueDeclare(queue, true, false, false, null);
        } catch (IOException e) {
            LOG.debug("Error while connecting to the " + host + "/" + queue, e);
            throw new RequestConnectException(e, URI.create("amqp://" + host + "/" + queue));
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    public DefaultRabbitMQSender(final String host, final String queue, final int timeout) throws RequestConnectException {
        this(host, "", "", queue, timeout, true);
    }

    public DefaultRabbitMQSender(final String host, final String queue) throws RequestConnectException {
        this(host, queue, 0);
    }

    @Override
    public void send(final byte[] msg, final AMQP.BasicProperties properties) throws IOException {
        LOG.debug("Sending message with length " + (msg != null ? msg.length : 0) + " to " + host + "" + queue);
        channel.basicPublish("", queue, properties, msg);
    }

    @Override
    public void send(final byte[] msg) throws IOException {
        send(msg, createProperties());
    }

    @Override
    public void send(final ByteString msg, final AMQP.BasicProperties properties) throws IOException {
        send(msg.toByteArray(), properties);
    }

    @Override
    public void send(final ByteString msg) throws IOException {
        send(msg.toByteArray(), createProperties());
    }

    @Override
    public void send(final MessageLite msg, final AMQP.BasicProperties properties) throws IOException {
        send(msg.toByteArray(), properties);
    }

    @Override
    public void send(final MessageLite msg) throws IOException {
        send(msg.toByteArray(), createProperties());
    }

    public AMQP.BasicProperties createProperties(String msgType, String contentType, String expiration) {
        return new AMQP.BasicProperties.Builder()
                .expiration(expiration)
                .contentType(contentType)
                .contentEncoding("utf-8")
                .deliveryMode(2)
                .priority(0)
                .type(msgType)
                .timestamp(new Date())
                .build();
    }

    public AMQP.BasicProperties createProperties(String msgType, String contentType) {
        return createProperties(msgType, contentType, null);
    }

    public AMQP.BasicProperties createProperties(String msgType) {
        return createProperties(msgType, "application/octet-stream", null);
    }

    public AMQP.BasicProperties createProperties() {
        return createProperties(null, "application/octet-stream", null);
    }

    private ExceptionHandler getExceptionHandler() {
        return new ExceptionHandler() {
            @Override
            public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
                LOG.warn("Error in connection driver", exception);
            }

            @Override
            public void handleReturnListenerException(Channel channel, Throwable exception) {
                LOG.warn("Error in ReturnListener", exception);
            }

            @Override
            public void handleFlowListenerException(Channel channel, Throwable exception) {
                LOG.warn("Error in FlowListener", exception);
            }

            @Override
            public void handleConfirmListenerException(Channel channel, Throwable exception) {
                LOG.warn("Error in ConfirmListener", exception);
            }

            @Override
            public void handleBlockedListenerException(Connection connection, Throwable exception) {
                LOG.warn("Error in BlockedListener", exception);
            }

            @Override
            public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
                LOG.warn("Error in consumer", exception);
            }

            @Override
            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
                LOG.warn("Error in connection recovery", exception);
            }

            @Override
            public void handleChannelRecoveryException(Channel ch, Throwable exception) {
                LOG.warn("Error in channel recovery", exception);
            }

            @Override
            public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
                LOG.warn("Error in topology recovery", exception);
            }
        };
    }
}
