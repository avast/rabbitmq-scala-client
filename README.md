# RabbitMQ client
This client is lightweight wrapper over standard [RabbitMQ java client](https://www.rabbitmq.com/java-client.html).
It's API may be difficult to use for inexperienced RabbitMQ users. Goal of this library is to simplify basic use cases and shadow the programmer
from the underlying client.

Currently only Scala API is available. Contact the author for discussion about Java API if you want it or look at the example below.

Author: [Jenda Kolena](mailto:kolena@avast.com)

## Dependency
`compile 'com.avast.clients:rabbitmq-client_?:2.0.1'`
For most current version see the [Teamcity](https://teamcity.int.avast.com/viewType.html?buildTypeId=CloudSystems_RabbitMQClient_ReleasePublish).

## Usage

### Configuration

```hocon
myConfig {
  hosts = ["localhost:5672"]
  virtualHost = "/"
  
  name="Cluster01Connection" // used for logging AND is also visible in client properties in RabbitMQ management console

  ssl {
    enabled = false // enabled by default
  }

  credentials {
    //enabled = true // enabled by default

    username = "guest"
    password = "guest"
  }

  connectionTimeout = 5s // default value

  networkRecovery {
    enabled = true // default value
    period = 5s // default value
  }


  // CONSUMERS AND PRODUCERS:

  // this is the name you use while creating; it's recommended to use something more expressive, like "licensesConsumer"
  consumer {
    name = "Testing" // this is used for metrics, logging etc.

    consumerTag = Default // string or "Default"; default is randomly generated string (like "amq.ctag-ov2Sp8MYKE6ysJ9SchKeqQ"); visible in RabbitMQ management console

    queueName = "test"

    prefetchCount = 100 // don't change unless you have a reason to do so ;-)

    // should the consumer declare queue he wants to read from?
    declare {
      enabled = true // disabled by default

      durable = true // default value
      autoDelete = false // default value
      exclusive = false // default value
    }

    // bindings from exchanges to the queue
    bindings = [
      {
        // all routing keys the queue should bind with
        // leave empty or use "" for binding to fanout exchange
        routingKeys = ["test"]

        // should the consumer declare exchange he wants to bind to?
        exchange {
          name = "myclient"

          declare {
            enabled = true // disabled by default

            type = "direct" // fanout, topic
          }
        }
      }
    ]
  }

  // this is the name you use while creating; it's recommended to use something more expressive, like "licensesProducer"
  producer {
    name = "Testing" // this is used for metrics, logging etc.

    exchange = "myclient"

    // should the consumer declare exchange he wants to send to?
    declare {
      enabled = true // disabled by default

      type = "direct" // fanout, topic
      durable = true // default value
      autoDelete = false // default value
    }
  }
}
```
For full list of options please see [reference.conf](src/main/resources/reference.conf).

As you may have noticed, there are `producer` and `consumer` configurations *inside* the `myConfig` block. Even though they are NOT dependent and they don't
have to be this structured, it seems like a good strategy to have all producers/consumers in block which configures connection to the RabbitMQ server. In case
there are more of them, proper naming like `producer-testing` should be used.

### Scala usage

```scala
  val config = ConfigFactory.load().getConfig("myConfig")

  // you need both `ExecutorService` (optionally passed to `RabbitMQChannelFactory`) and `ExecutionContext` (implicitly passed to consumer), both are
  // used for callbacks execution, so why not to use a `ExecutionContextExecutionService`?
  implicit val ex: ExecutionContextExecutorService = ???

  val monitor = new JmxMetricsMonitor("TestDomain")

  // here you create the channel factory; by default, use it for all producers/consumers amongst one RabbitMQ server - they will share a single TCP connection
  // but have separated channels
  // if you expect very high load, you can
  val channelFactory = RabbitMQChannelFactory.fromConfig(config, Some(ex))

  val receiver = RabbitMQClientFactory.Consumer.fromConfig(config.getConfig("consumer"), channelFactory, monitor) { delivery =>
    println(delivery)
    Future.successful(true)
  }

  val sender = RabbitMQClientFactory.Producer.fromConfig(config.getConfig("producer"), channelFactory, monitor)
```

### Java usage

The Java api is placed in subpackage `javaapi` (but not all classes have their Java counterparts, some have to be imported from Scala API,
depending on your usage).  
Don't get confused by the Java API actually implemented in Scala.

```java
final RabbitMQChannelFactory rabbitMQChannelFactory = RabbitMQChannelFactory.fromConfig(config).withExecutor(executor).build();

final RabbitMQConsumer rabbitMQConsumer = RabbitMQClientFactory.createConsumerfromConfig(
    config.getConfig("consumer"),
    rabbitMQChannelFactory,
    NoOpMonitor.INSTANCE,
    null,
    executor,
    ExampleJava::handleDelivery
);

final RabbitMQProducer rabbitMQProducer = RabbitMQClientFactory.createProducerfromConfig(
    config.getConfig("producer"),
    rabbitMQChannelFactory,
    NoOpMonitor.INSTANCE
);

```

See [full example](/src/test/java/ExampleJava.java)

## Notes

### Structured config
It's highly recommended to have the config structured ad in the example, that means:
```hocon
rabbitConfig {
  // connection config
  
  consumer1 {
    //consumer config
  }
  
  consumer2 {
    //consumer config
  }
  
  producer1 {
    //consumer config
  }
  
  producer2 {
    //consumer config
  }
}

```
It usually leads to much more clear config.

### DeliveryResult
The consumers `readAction` returns `Future` of `DeliveryResult`. The `DeliveryResult` has 4 possible values
(descriptions of usual use-cases):
1. Ack - the message was processed; it will be removed form the queue
1. Reject - the message is corrupted or for some other reason we don't want to see it again; it will be removed from the queue
1. Retry - the message couldn't be processed at this moment (unreachable 3rd party services?); it will be requeued (inserted on the top of
the queue)
1. Republish - the message may be corrupted but we're not sure; it will be re-published to the bottom of the queue (as a new message and the
original one will be removed). It's usually wise to use some customized header as a counter to prevent an infinite republishing of the message.

####Difference between _Retry_ and _Republish_
When using _Retry_ the message can effectively cause starvation of other messages in the queue
until the message itself can be processed; on the other hand _Republish_ inserts the message to the original queue as a new message and it
lets the consumer handle other messages (if they can be processed).
