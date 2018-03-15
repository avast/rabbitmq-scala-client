import com.avast.bytes.Bytes;
import com.avast.clients.rabbitmq.javaapi.*;
import com.avast.metrics.test.NoOpMonitor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class ExampleJava {
    public static void main(String[] args) {
        final Config config = ConfigFactory.load().getConfig("myConfig");
        final String routingKey = config.getString("consumer.queueName");

        final ExecutorService executor = Executors.newCachedThreadPool();
        final ForkJoinPool callbackExecutor = new ForkJoinPool();

        final RabbitMQJavaConnection connection = RabbitMQJavaConnection.newBuilder(config, executor).build();

        final RabbitMQConsumer rabbitMQConsumer = connection.newConsumer(
                "consumer",
                NoOpMonitor.INSTANCE,
                callbackExecutor,
                ExampleJava::handleDelivery
        );

        final RabbitMQProducer rabbitMQProducer = connection.newProducer(
                "producer",
                NoOpMonitor.INSTANCE,
                callbackExecutor
        );

        for (int i = 0; i < 1000; i++) {
            try {
                rabbitMQProducer.send(routingKey, Bytes.copyFromUtf8("hello world")).get();
            } catch (Exception e) {
                System.err.println("Message could not be sent: " + e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }

    private static CompletableFuture<DeliveryResult> handleDelivery(Delivery delivery) {
        System.out.println(delivery.getBody().toStringUtf8());
        return CompletableFuture.completedFuture(DeliveryResult.Ack());
    }
}
