package com.avast.client.rabbitmq;

import com.avast.client.api.GenericAsyncHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public class DefaultRabbitMQReceiver implements RabbitMQReceiver {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("mqreceiver-%d").build());

    protected final String queue;
    protected final Channel channel;
    protected final QueueingConsumer consumer;

    protected final Set<GenericAsyncHandler<QueueingConsumer.Delivery>> listeners = new LinkedHashSet<>(2);
    protected final Semaphore listenerMutex = new Semaphore(0);

    protected final Set<Long> failedTags = new LinkedHashSet<>(2);
    protected final boolean allowRetry;

    protected Thread listenerThread;


    public DefaultRabbitMQReceiver(final String host, final String queue, final boolean allowRetry, final int connectionTimeout) throws IOException {
        this.queue = queue;
        this.allowRetry = allowRetry;

        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setSharedExecutor(executor);
        factory.setExceptionHandler(getExceptionHandler());
        factory.setConnectionTimeout(connectionTimeout > 0 ? connectionTimeout : 5000);

        channel = factory.newConnection().createChannel();

        channel.queueDeclare(queue, true, false, false, null);

        consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, false, consumer);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (listenerThread == null || !listenerThread.isAlive()) {
                    planListener(); //start or restart the listener
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public DefaultRabbitMQReceiver(final String host, final String queue) throws IOException {
        this(host, queue, true, 0);
    }

    @Override
    public void addListener(final GenericAsyncHandler<QueueingConsumer.Delivery> listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
        listenerMutex.release(100000);//for sure
    }

    protected synchronized void planListener() {
        LOG.debug("Waiting for listeners");
        try {
            listenerMutex.acquire();//just wait for some listener
            listenerMutex.release();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        listenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.debug("Waiting for message");
                try {
                    final QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                    final long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                    LOG.debug("Received message, length " + delivery.getBody().length + "B");

                    boolean error = false;

                    final Set<GenericAsyncHandler<QueueingConsumer.Delivery>> listeners;
                    synchronized (DefaultRabbitMQReceiver.this.listeners) {
                        listeners = new HashSet<>(DefaultRabbitMQReceiver.this.listeners);
                    }
                    for (GenericAsyncHandler<QueueingConsumer.Delivery> listener : listeners) {
                        try {
                            listener.completed(delivery);
                        } catch (Exception e) {
                            LOG.info("Error while executing the listener", e);
                            error = true;

                            if (allowRetry) {
                                if (failedTags.contains(deliveryTag)) { //when it has failed before, ACK it, or add to retry list
                                    LOG.warn("Processing of listener has failed");
                                    ack(deliveryTag);
                                }
                                else {
                                    LOG.debug("Processing of listener has failed, but retry is allowed");
                                    failedTags.add(deliveryTag);
                                }
                            }
                            else {
                                LOG.warn("Processing of listener has failed");
                                ack(deliveryTag);
                            }
                        }
                    }

                    if (!error) {
                        ack(deliveryTag);
                    }
                } catch (Exception e) {
                    LOG.debug("Error while receiving new message", e);
                    final Set<GenericAsyncHandler<QueueingConsumer.Delivery>> listeners = new HashSet<>(DefaultRabbitMQReceiver.this.listeners);
                    for (GenericAsyncHandler<QueueingConsumer.Delivery> listener : listeners) {
                        listener.failed(e);
                    }
                }
            }
        }, "mqlistener-" + queue + "-" + (System.currentTimeMillis() / 1000));
        listenerThread.start();
    }

    protected void ack(long tag) {
        LOG.debug("Sending ACK to message with tag " + tag);
        try {
            channel.basicAck(tag, false);
        } catch (IOException e) {
            LOG.warn("Cannot ACK message with tag " + tag, e);
        }
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
