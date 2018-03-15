import com.avast.bytes.Bytes;
import com.avast.clients.rabbitmq.javaapi.*;
import com.avast.metrics.test.NoOpMonitor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExampleJava {
    public static void main(String[] args) {
        File file = new File("/home/jenda/dev/rabbitmqclient/localhost.conf").getAbsoluteFile();
        Config config = ConfigFactory.parseFile(file).getConfig("myConfig");
        String routingKey = config.getString("consumer.queueName");


        final ExecutorService executor = Executors.newCachedThreadPool();

        final RabbitMQJavaConnection connection = RabbitMQJavaConnection.newBuilder(config, executor).build();

        final RabbitMQConsumer rabbitMQConsumer = connection.newConsumer(
                "consumer",
                NoOpMonitor.INSTANCE,
                executor,
                ExampleJava::handleDelivery
        );

        final RabbitMQProducer rabbitMQProducer = connection.newProducer("producer",
                NoOpMonitor.INSTANCE,
                executor
        );

        for (int i = 0; i < 1000; i++) {
            try {
                rabbitMQProducer.send(routingKey, Bytes.copyFromUtf8("hello world")).get();
            } catch (Exception e) {
                System.err.println("Message could not be sent: " + e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }

    public static CompletableFuture<DeliveryResult> handleDelivery(Delivery delivery) {
        System.out.println(delivery.getBody().toStringUtf8());
        return CompletableFuture.completedFuture(DeliveryResult.Ack());
    }
}
