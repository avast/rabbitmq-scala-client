package com.avast.client.rabbitmq;

import com.avast.client.api.exceptions.RequestConnectException;
import com.avast.jmx.JMXProperty;
import com.avast.jmx.MyDynamicBean;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created <b>4.12.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
abstract class RabbitMQClientBase implements RabbitMQClient {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(RabbitMQClient.class.getSimpleName().toLowerCase() + "-%d").setDaemon(true).build());

    @JMXProperty
    protected final String queue;
    protected final AutorecoveringChannel channel;

    @JMXProperty
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    @JMXProperty
    protected final Address[] addresses;

    protected final String jmxGroup, jmxType, clientType;

    protected RabbitMQClientBase(final String clientType, final Address[] addresses, final String virtualHost, final String username, final String password, final String queue, final int connectionTimeout, final int recoveryTimeout, final SSLContext sslContext, final ExceptionHandler exceptionHandler, final String jmxGroup) throws RequestConnectException {
        this.queue = queue;

        try {
            final ConnectionFactory factory = new ConnectionFactory();
//            if (host.contains("/")) {
//                final String[] parts = host.split("/");
//                if (parts.length > 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
//                    throw new IllegalArgumentException("Invalid definition of host/virtualhost");
//                }
//                factory.setHost(parts[0]);
//                factory.setVirtualHost(parts[1]);
//            } else {
//                factory.setHost(host);
//            }

            factory.setVirtualHost(virtualHost);
            this.addresses = addresses;
            if (sslContext != null) factory.useSslProtocol(sslContext);

            factory.setSharedExecutor(executor);
            factory.setExceptionHandler(exceptionHandler != null ? exceptionHandler : getExceptionHandler());
            factory.setConnectionTimeout(connectionTimeout > 0 ? connectionTimeout : 5000);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(recoveryTimeout > 0 ? recoveryTimeout : 5000);

            if (StringUtils.isNotBlank(username)) {
                factory.setUsername(username);
            }
            if (StringUtils.isNotBlank(password)) {
                factory.setPassword(password);
            }

            LOG.info("Connecting to RabbitMQ on " + addresses + "/" + queue);
            final AutorecoveringConnection connection = (AutorecoveringConnection) factory.newConnection(addresses);
            channel = (AutorecoveringChannel) connection.createChannel();
            LOG.debug("Connected to " + addresses + "/" + queue);

            connection.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    LOG.debug("Shutdown of RabbitMQ server detected", cause);
                }
            });

            connection.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    try {
                        onChannelRecovered(recoverable);
                        LOG.info("Connection to " + addresses + " has been recovered");
                    } catch (Exception e) {
                        LOG.error("Error while recovering the client", e);
                    }
                }
            });


            channel.queueDeclare(queue, true, false, false, null);

            jmxType = factory.getHost() + (StringUtils.isNotBlank(factory.getVirtualHost()) ? "/" + factory.getVirtualHost() : "");
            this.jmxGroup = jmxGroup;
            this.clientType = clientType;

            MyDynamicBean.exposeAndRegisterSilently(jmxGroup + ":type=" + jmxType + ",scope=" + queue + "(" + clientType + "),name=client", this);
        } catch (IOException e) {
            LOG.debug("Error while connecting to the " + addresses + "/" + queue, e);
            throw new RequestConnectException(e, URI.create("amqp://" + addresses + "/" + queue), 0);
        }
    }

    protected abstract void onChannelRecovered(Recoverable recoverable);

    protected MetricName getMetricName(final String name) {
        return new MetricName(jmxGroup, jmxType, name, queue + "(" + clientType + ")");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.get()) return;

        closed.set(true);
        channel.close();
    }

    @Override
    public synchronized void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            LOG.warn("Error while closing the receiver", e);
        }
    }

    @Override
    public synchronized boolean isClosed() {
        return closed.get();
    }

    @JMXProperty(name = "alive")
    @Override
    public synchronized boolean isAlive() {
        return !isClosed() && channel.isOpen();
    }

    protected ExceptionHandler getExceptionHandler() {
        return new ExceptionHandler() {
            @Override
            public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
                LOG.warn("Error in connection to " + conn.getAddress().getHostName(), exception);
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
                LOG.warn("Error in consumer " + consumer, exception);
            }

            @Override
            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
                LOG.warn("Connection to " + conn.getAddress().getHostName() + " couldn't be recovered: " + exception.getClass().getName() + "(" + exception.getMessage() + ")");
            }

            @Override
            public void handleChannelRecoveryException(Channel ch, Throwable exception) {
                LOG.warn("Channel couldn't be recovered", exception);
            }

            @Override
            public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
                LOG.warn("Topology couldn't be recovered", exception);
            }
        };
    }
}
