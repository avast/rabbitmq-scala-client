import com.avast.bytes.Bytes;
import com.avast.clients.rabbitmq.javaapi.*;
import com.avast.metrics.test.NoOpMonitor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.concurrent.*;

public class ExampleJava {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

        final RabbitMQPullConsumer rabbitMQPullConsumer = connection.newPullConsumer(
                "consumer",
                NoOpMonitor.INSTANCE,
                callbackExecutor
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

        /* Pull consumer: */

        final PullResult pullResult = rabbitMQPullConsumer.pull().get();

        if (pullResult.isOk()) {
            PullResult.Ok result = (PullResult.Ok) pullResult;

            final DeliveryWithHandle deliveryWithHandle = result.getDeliveryWithHandle();

            deliveryWithHandle.handle(DeliveryResult.Ack()).get();
        }
    }

    private static CompletableFuture<DeliveryResult> handleDelivery(Delivery delivery) {
        System.out.println(delivery.getBody().toStringUtf8());
        return CompletableFuture.completedFuture(DeliveryResult.Ack());
    }
}
