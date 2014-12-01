package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.AMQP;
import org.apache.commons.lang.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
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

    /* ---------------------------------------------------------------- */

    @SuppressWarnings("unused")
    public static class Builder {
        protected String host = null, virtualHost = "", username = null, password = null, queue = null;
        protected int connectTimeout = 5000;
        protected SSLContext sslContext = null;
        protected boolean allowRetry = false;

        public Builder(String host, String queue) {
            if (StringUtils.isBlank(host)) throw new IllegalArgumentException("Host must not be null");
            if (StringUtils.isBlank(queue)) throw new IllegalArgumentException("Queue name must not be null");

            this.host = host;
            this.queue = queue;
        }

        public static Builder create(String host, String queue) {
            return new Builder(host, queue);
        }

        public Builder withVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
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

        public Builder withRetry() {
            allowRetry = true;
            return this;
        }

        public DefaultRabbitMQSender build() throws RequestConnectException {
            return new DefaultRabbitMQSender(host + "/" + virtualHost, Strings.nullToEmpty(username), Strings.nullToEmpty(password), queue, connectTimeout, sslContext);
        }
    }
}
