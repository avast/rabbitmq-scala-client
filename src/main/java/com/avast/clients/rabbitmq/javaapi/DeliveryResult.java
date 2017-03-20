package com.avast.clients.rabbitmq.javaapi;

public enum DeliveryResult {
    /** Success while processing the message - it will be removed from the queue. */
    Ack,
    /** Reject the message from processing - it will be removed (discarded). */
    Reject,
    /** The message cannot be processed but is worth - it will be requeued to the top of the queue. */
    Retry,
    /** The message cannot be processed but is worth - it will be requeued to the bottom of the queue. */
    Republish
}
