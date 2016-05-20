# RabbitMQ client
This client is lightweight wrapper over standard [RabbitMQ java client](https://www.rabbitmq.com/java-client.html).
It's API may be difficult to use for inexperienced RabbitMQ users. Goal of this library is to simplify basic use cases and shadow the programmer
from the underlying client.

Author: [Jenda Kolena](kolena@avast.com)

## Dependency
`compile 'com.avast.clients:rabbitmq-client_?:1.0.1'`

## Usage
```scala
  val config = ConfigFactory.load().getConfig("myConfig")

  // you need both `ExecutionService` (optionally passed to `RabbitMQChannelFactory`) and `ExecutionContext` (implicitly passed to consumer), both are
  // used for callbacks execution, so why not to use a `ExecutionContextExecutionService`?
  implicit val ex: ExecutionContextExecutionService = ???

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

This is how to configuration should look like:
```
myConfig {
  hosts = ["localhost:5672"]
  virtualHost = "/"

  ssl {
    enabled = false // enabled by default
  }

  credentials {
    //enabled = true // enabled by default

    username = "guest"
    password = "guest"
  }

  connectionTimeout = 5s // default value


  // CONSUMERS AND PRODUCERS:

  consumer {
    name = "Testing"

    queueName = "test"

    // should the consumer declare queue he wants to read from?
    declare {
      enabled = true // disabled by default

      durable = true // default value
      autoDelete = false // default value
      exclusive = false // default value
    }

    // bindings from exchanges to the queue
    binds = [
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

  producer {
    name = "Testing"

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
