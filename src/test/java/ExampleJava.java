import com.avast.bytes.Bytes;
import com.avast.clients.rabbitmq.api.RabbitMQConsumer;
import com.avast.clients.rabbitmq.api.RabbitMQProducer;
import com.avast.clients.rabbitmq.javaapi.Delivery;
import com.avast.clients.rabbitmq.javaapi.DeliveryResult;
import com.avast.clients.rabbitmq.javaapi.RabbitMQFactory;
import com.avast.clients.rabbitmq.javaapi.RabbitMQJavaFactory;
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

        final RabbitMQJavaFactory factory = RabbitMQFactory.newBuilder(config).withExecutor(executor).build();

        final RabbitMQConsumer rabbitMQConsumer = factory.newConsumer(
                "consumer",
                NoOpMonitor.INSTANCE,
                executor,
                ExampleJava::handleDelivery
        );

        final RabbitMQProducer rabbitMQProducer = factory.newProducer("producer",
                NoOpMonitor.INSTANCE
        );

        for (int i = 0; i < 1000; i++) {
            rabbitMQProducer.send(routingKey, Bytes.copyFromUtf8("hello world"));
        }
    }

    public static CompletableFuture<DeliveryResult> handleDelivery(Delivery delivery) {
        System.out.println(delivery.getBody().toStringUtf8());
        return CompletableFuture.completedFuture(DeliveryResult.Ack());
    }
}
