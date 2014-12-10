package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ExceptionHandler;
import org.apache.commons.lang.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQSender extends RabbitMQClient {

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

    /* ---------------------------------------------------------------- */

    @SuppressWarnings("unused")
    public static class Builder {
        protected final Address[] addresses;
        protected String host = null, virtualHost = "", username = null, password = null, queue = null, jmxGroup = RabbitMQSender.class.getPackage().getName();
        protected int connectTimeout = 5000, recoveryTimeout = 5000;
        protected SSLContext sslContext = null;

        protected ExceptionHandler exceptionHandler = null;

        public Builder(Address[] addresses, String queue) {
            if (addresses.length == 0) throw new IllegalArgumentException("Addresses must not be empty");
            if (StringUtils.isBlank(queue)) throw new IllegalArgumentException("Queue name must not be null");

            this.addresses = addresses;
            this.queue = queue;
        }

        public static Builder create(Address[] addresses, String queue) {
            return new Builder(addresses, queue);
        }

        public Builder withVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }


        public Builder withJmxGroup(String jmxGroup) {
            this.jmxGroup = jmxGroup;
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

        public Builder withSslContextFromKeystore(Path keystorePath, String password) throws IOException {
            try {
                if (!Files.isReadable(keystorePath))
                    throw new FileNotFoundException("Keystore file '" + keystorePath + "' cannot be found or is not readable");

                final SSLContext context = SSLContext.getInstance("TLS");
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                final KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(Files.newInputStream(keystorePath), password.toCharArray());

                final TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
                tmf.init(ks);

                kmf.init(ks, "".toCharArray());
                context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                this.sslContext = context;
            } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
                throw new RuntimeException(e);
            }

            return this;
        }

        public DefaultRabbitMQSender build() throws RequestConnectException {
            return new DefaultRabbitMQSender(addresses, virtualHost, Strings.nullToEmpty(username), Strings.nullToEmpty(password), queue, connectTimeout, recoveryTimeout, sslContext, exceptionHandler, jmxGroup);
        }
    }
}
