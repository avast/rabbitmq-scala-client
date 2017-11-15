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
    public static void main(String[] args) throws InterruptedException {
//        File file = new File("/home/jenda/dev/rabbitmqclient/localhost.conf").getAbsoluteFile();
//        Config config = ConfigFactory.parseFile(file).getConfig("myConfig");
//        String routingKey = config.getString("consumer.queueName");

        File file = new File("./test.conf").getAbsoluteFile();
        Config producerConfig = ConfigFactory.parseFile(file).getConfig("testProducer");
        Config consumerConfig = ConfigFactory.parseFile(file).getConfig("testConsumer");


        final ExecutorService executor = Executors.newCachedThreadPool();


        final RabbitMQJavaFactory clientFactory = RabbitMQFactory.newBuilder(consumerConfig).withExecutor(executor).build();
        final RabbitMQConsumer rabbitMQConsumer = clientFactory.newConsumer(
                "consumer",
                NoOpMonitor.INSTANCE,
                executor,
                ExampleJava::handleDelivery
        );

//        final RabbitMQJavaFactory producerFactory = RabbitMQFactory.newBuilder(producerConfig).withExecutor(executor).build();
//        final RabbitMQProducer rabbitMQProducer = producerFactory.newProducer("producer",
//                NoOpMonitor.INSTANCE
//        );
        Thread.sleep(10000);

        for (int i = 0; i < 1000; i++) {
            try {
//                rabbitMQProducer.send(routingKey, Bytes.copyFromUtf8("hello world"));
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
