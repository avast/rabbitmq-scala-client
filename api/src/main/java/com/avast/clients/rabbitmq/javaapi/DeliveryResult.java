package com.avast.clients.rabbitmq.javaapi;

import java.util.Collections;
import java.util.Map;

public class DeliveryResult {
    /**
     * Success while processing the message - it will be removed from the queue.
     */
    public static Ack Ack() {
        return Ack.INSTANCE;
    }

    /**
     * Reject the message from processing - it will be removed (discarded).
     */
    public static Reject Reject() {
        return Reject.INSTANCE;
    }

    /**
     * The message cannot be processed but is worth - it will be requeued to the top of the queue.
     */
    public static Retry Retry() {
        return Retry.INSTANCE;
    }

    /**
     * The message cannot be processed but is worth - it will be requeued to the bottom of the queue.
     */
    public static Republish Republish(final Map<String, Object> newHeaders) {
        return new Republish(newHeaders);
    }


    /**
     * Success while processing the message - it will be removed from the queue.
     */
    public static class Ack extends DeliveryResult {
        static Ack INSTANCE = new Ack();
    }

    /**
     * Reject the message from processing - it will be removed (discarded).
     */
    public static class Reject extends DeliveryResult {
        static Reject INSTANCE = new Reject();
    }

    /**
     * The message cannot be processed but is worth - it will be requeued to the top of the queue.
     */
    public static class Retry extends DeliveryResult {
        static Retry INSTANCE = new Retry();
    }

    /**
     * The message cannot be processed but is worth - it will be requeued to the bottom of the queue.
     */
    public static class Republish extends DeliveryResult {
        private final Map<String, Object> newHeaders;

        public Republish(final Map<String, Object> newHeaders) {
            this.newHeaders = newHeaders;
        }

        public Republish() {
            this.newHeaders = Collections.emptyMap();
        }

        public Map<String, Object> getNewHeaders() {
            return newHeaders;
        }
    }
}
