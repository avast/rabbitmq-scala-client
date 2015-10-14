package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.avast.client.encryption.SSLBuilder;
import com.avast.metrics.api.Monitor;
import com.avast.metrics.test.NoOpMonitor;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ExceptionHandler;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

/**
 * @author Jenda Kolena, kolena@avast.com
 */
@SuppressWarnings("unused")
public interface RabbitMQSender extends RabbitMQClient {

    /**
     * Sends message to the queue.
     *
     * @param exchange   Name of the exchange.
     * @param routingKey The routing key
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, final String routingKey, final byte[] msg, final AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param exchange   Name of the exchange.
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, final byte[] msg, final AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param exchange Name of the exchange.
     * @param msg      The message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, final byte[] msg) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param exchange   Name of the exchange.
     * @param routingKey The routing key
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, final String routingKey, MessageLite msg, AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param exchange   Name of the exchange.
     * @param msg        The message.
     * @param properties Properties related to the message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, MessageLite msg, AMQP.BasicProperties properties) throws IOException;

    /**
     * Sends message to the queue.
     *
     * @param exchange Name of the exchange.
     * @param msg      The message.
     * @throws IOException When problem while sending has occurred.
     */
    void send(final String exchange, MessageLite msg) throws IOException;


    /* ---------------------------------------------------------------- */

    @NotThreadSafe
    class Builder {
        protected final Address[] addresses;
        protected String host = null, virtualHost = "", username = null, password = null, name = System.currentTimeMillis() + "";
        protected int connectTimeout = 5000, recoveryTimeout = 5000;
        protected SSLContext sslContext = null;
        protected RabbitMQDeclare declare = null;
        protected Monitor monitor = NoOpMonitor.INSTANCE;

        protected ExceptionHandler exceptionHandler = null;

        public Builder(Address[] addresses) {
            if (ArrayUtils.isEmpty(addresses)) throw new IllegalArgumentException("Addresses must not be empty");

            this.addresses = addresses;
        }

        public static Builder createFromHostsString(String hostsString) {
            return new Builder(Address.parseAddresses(hostsString));
        }

        public static Builder create(Address[] addresses) {
            return new Builder(addresses);
        }

        public static Builder create(Collection<Address> addresses) {
            return new Builder(Iterables.toArray(addresses, Address.class));
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder withMonitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public Builder withCredentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder withConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder withDeclare(RabbitMQDeclare declare) {
            this.declare = declare;
            return this;
        }

        public Builder withRecoveryTimeout(int recoveryTimeout) {
            this.recoveryTimeout = recoveryTimeout;
            return this;
        }

        public Builder withExceptionHandler(ExceptionHandler exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public Builder withSslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public Builder withSslContextFromKeystore(Path keystorePath, String password) {
            return withSslContext(SSLBuilder.create().setKeyStore(keystorePath, password).build());
        }

        public RabbitMQSender build() throws RequestConnectException {
            return new DefaultRabbitMQSender(addresses, virtualHost, Strings.nullToEmpty(username), Strings.nullToEmpty(password), connectTimeout, recoveryTimeout, sslContext, exceptionHandler, declare, monitor, name);
        }
    }
}
