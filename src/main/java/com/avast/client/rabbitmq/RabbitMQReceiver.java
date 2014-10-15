package com.avast.client.rabbitmq;

import com.avast.client.api.GenericAsyncHandler;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Created <b>15.10.2014</b><br>
 *
 * @author Jenda Kolena, kolena@avast.com
 */
public interface RabbitMQReceiver {
    void addListener(GenericAsyncHandler<QueueingConsumer.Delivery> listener);
}
