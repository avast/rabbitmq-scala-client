package com.avast.client.rabbitmq;

import com.avast.client.api.GenericAsyncHandler;
import com.avast.client.api.exceptions.RequestConnectException;
import com.avast.client.encryption.SSLBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQReceiver extends RabbitMQClient {

    /**
     * Adds listener to this MQ receiver.
     *
     * @param listener The listener.
     */
    void setListener(GenericAsyncHandler<QueueingConsumer.Delivery> listener);

    /* ---------------------------------------------------------------- */

    @SuppressWarnings("unused")
    public static class Builder {
        protected final Address[] addresses;
        protected String virtualHost = "", username = null, password = null, queue = null, jmxGroup = RabbitMQReceiver.class.getPackage().getName();
        protected int connectTimeout = 5000, recoveryTimeout = 5000;
        protected SSLContext sslContext = null;
        protected ExceptionHandler exceptionHandler = null;
        protected boolean allowRetry = false;

        public Builder(Address[] addresses, String queue) {
            if (ArrayUtils.isEmpty(addresses)) throw new IllegalArgumentException("Addresses must not be empty");
            if (StringUtils.isBlank(queue)) throw new IllegalArgumentException("Queue name must not be null");

            this.addresses = addresses;
            this.queue = queue;
        }

        public static Builder createFromHostsString(String hostsString, String queue) {
            return new Builder(Address.parseAddresses(hostsString), queue);
        }

        public static Builder create(Address[] addresses, String queue) {
            return new Builder(addresses, queue);
        }

        public static Builder create(Collection<Address> addresses, String queue) {
            return new Builder(Iterables.toArray(addresses, Address.class), queue);
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

        public Builder allowRetry(boolean allow) {
            allowRetry = allow;
            return this;
        }

        public Builder withRetry() {
            return allowRetry(true);
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

        public Builder withSslContextFromKeystore(Path keystorePath, String password) {
            return withSslContext(SSLBuilder.create().setKeyStore(keystorePath, password).build());
        }

        public RabbitMQReceiver build() throws RequestConnectException {
            return new DefaultRabbitMQReceiver(addresses, virtualHost, Strings.nullToEmpty(username), Strings.nullToEmpty(password), queue, allowRetry, connectTimeout, recoveryTimeout, sslContext, exceptionHandler, jmxGroup);
        }
    }
}
