# RabbitMQ client

[![](https://teamcity.int.avast.com/app/rest/builds/buildType:(id:CloudSystems_RabbitMQClient_Build)/statusIcon)] (https://teamcity.int.avast.com/viewType.html?buildTypeId=CloudSystems_RabbitMQClient_Build)

![](http://badges.ff.int.avast.com/image/maven-local/com.avast.clients/rabbitmq-client-core_2.12?title=RabbitMQClient)

This client is lightweight wrapper over standard [RabbitMQ java client](https://www.rabbitmq.com/java-client.html).
It's API may be difficult to use for inexperienced RabbitMQ users. Goal of this library is to simplify basic use cases and shadow the programmer
from the underlying client.

It uses [Lyra library](https://github.com/jhalterman/lyra) for better recovery ability.

Author: [Jenda Kolena](mailto:kolena@avast.com)

## Dependency
`compile 'com.avast.clients:rabbitmq-client-core_?:x.x.x'`
For most current version see the [Teamcity](https://teamcity.int.avast.com/viewType.html?buildTypeId=CloudSystems_RabbitMQClient_ReleasePublish).

## Modules

There are [api](api) and [core](core) modules available for the most common usage but there are also few _extras_ modules which contains
some optional functionality:
1. [extras](extras/README.md)
1. [extras-circe](extras-circe/README.md) (adds some circe-dependent functionality)
1. [extras-cactus](extras-cactus/README.md) (adds some cactus-dependent functionality)

## Usage

### Configuration

#### Structured config
Since v 5.x, it's necessary to have the config structured as following:
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
    //producer config
  }
  
  producer2 {
    //producer config
  }
}

```

#### Config example

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

    // should the producer declare exchange he wants to send to?
    declare {
      enabled = true // disabled by default

      type = "direct" // fanout, topic
      durable = true // default value
      autoDelete = false // default value
    }
  }
}
```
For full list of options please see [reference.conf](core/src/main/resources/reference.conf).

### Scala usage

```scala
  val config = ConfigFactory.load().getConfig("myRabbitConfig")

  // you need both `ExecutorService` (optionally passed to `RabbitMQFactory`) and `ExecutionContext` (implicitly passed to consumer), both are
  // used for callbacks execution, so why not to use a `ExecutionContextExecutionService`?
  implicit val ex: ExecutionContextExecutorService = ???

  val monitor = new JmxMetricsMonitor("TestDomain")

  // here you create the factory; it's shared for all producers/consumers amongst one RabbitMQ server - they will share a single TCP connection
  // but have separated channels
  // if you expect very high load, you can use separate connections for each producer/consumer, but it's usually not needed
  val rabbitFactory = RabbitMQFactory.fromConfig(config, Some(ex))

  val receiver = rabbitFactory.newConsumer("consumer", monitor) { delivery =>
    println(delivery)
    Future.successful(DeliveryResult.Ack)
  }

  val sender = rabbitFactory.newProducer("producer", monitor)
```

### Java usage

The Java api is placed in subpackage `javaapi` (but not all classes have their Java counterparts, some have to be imported from Scala API,
depending on your usage).  
Don't get confused by the Java API partially implemented in Scala.

```java
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

```

See [full example](core/src/test/java/ExampleJava.java)

## Notes

### Extras
There is a module with some optional functionality called [extras](extras/README.md).

### DeliveryResult
The consumers `readAction` returns `Future` of [`DeliveryResult`](api/src/main/scala/com/avast/clients/rabbitmq/api/DeliveryResult.scala). The `DeliveryResult` has 4 possible values
(descriptions of usual use-cases):
1. Ack - the message was processed; it will be removed from the queue
1. Reject - the message is corrupted or for some other reason we don't want to see it again; it will be removed from the queue
1. Retry - the message couldn't be processed at this moment (unreachable 3rd party services?); it will be requeued (inserted on the top of
the queue)
1. Republish - the message may be corrupted but we're not sure; it will be re-published to the bottom of the queue (as a new message and the
original one will be removed). It's usually wise  to prevent an infinite republishing of the message - see [Poisoned message handler](extras/README.md#poisoned-message-handler).

#### Difference between _Retry_ and _Republish_
When using _Retry_ the message can effectively cause starvation of other messages in the queue
until the message itself can be processed; on the other hand _Republish_ inserts the message to the original queue as a new message and it
lets the consumer handle other messages (if they can be processed).

### Bind/declare arguments
There is an option to specify bind/declare arguments for queues/exchanges as you may read about at [RabbitMQ docs](https://www.rabbitmq.com/queues.html).
Check [reference.conf](core/src/main/resources/reference.conf) or following example for usage:
```hocon
  producer {
    name = "Testing" // this is used for metrics, logging etc.

    exchange = "myclient"

    // should the producer declare exchange he wants to send to?
    declare {
      enabled = true // disabled by default

      type = "direct" // fanout, topic
      
      arguments = { "x-max-length" : 10000 }
    }
  }

```

### Additional declarations and bindings
Sometimes it's necessary to declare an additional queue or exchange which is not directly related to the consumers or producers you have
in your application (e.g. dead-letter queue).  
The library makes possible to do such thing, e.g.:
```scala
    rabbitMqFactory.bindExchange("backupExchangeBinding")
```
where the "backupExchangeBinding" is link to the configuration (use relative path to the factory configuration):
```hocon
    backupExchangeBinding {
      sourceExchangeName = "mainExchange"
      destExchangeName = "backupExchange"
      routingKeys = []
      arguments {}
    }
```
Check [reference.conf](core/src/main/resources/reference.conf) for all options or see [application.conf in tests](core/src/test/resources/application.conf).

### MultiFormatConsumer

Quite often you receive a single type of message but you want to receive it already decoded or even to support multiple formats of
the message. This is where `MultiTypeConsumer` could be used.  

Modules [extras-circe](extras-circe/README.md) and [extras-cactus](extras-cactus/README.md) provide support for JSON and GPB conversion. They
are both used in the example below.

Usage example:

[Proto file](core/src/test/proto/ExampleEvents.proto)

```scala
import com.avast.bytes.Bytes
import com.avast.cactus.bytes._ // Cactus support for Bytes, see https://github.com/avast/cactus#bytes
import com.avast.client.rabbitmq.test.ExampleEvents.{NewFileSourceAdded => NewFileSourceAddedGpb}
import com.avast.clients.rabbitmq.extras.multiformat._
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._ // to auto derive `io.circe.Decoder[A]` considering snake-case in JSON - https://circe.github.io/circe/codec.html#custom-key-mappings-via-annotations

import scala.collection.JavaConverters._

private implicit val c: Configuration = Configuration.default.withSnakeCaseMemberNames
private implicit val d: Decoder[Bytes] = Decoder.decodeString.map(Utils.hexToBytesImmutable)

case class FileSource(fileId: Bytes, source: String)

case class NewFileSourceAdded(fileSources: Seq[FileSource])

val consumer = MultiFormatConsumer.forType[NewFileSourceAdded](
  JsonFormatConverter.derive(), // requires implicit `io.circe.Decoder[NewFileSourceAdded]`
  GpbFormatConverter[NewFileSourceAddedGpb].derive() // requires implicit `com.avast.cactus.Converter[NewFileSourceAddedGpb, NewFileSourceAdded]`
)(
  businessLogic.processMessage,
  failureHandler
)
```
(see [unit test](core/src/test/scala/com/avast/clients/rabbitmq/MultiFormatConsumerTest.scala) for full example)

#### Implementing own `FormatConverter`

The [FormatConverter](core/src/main/scala/com/avast/clients/rabbitmq/FormatConverter.scala) is usually reacting to Content-Type (like in
the example below) but it's not required - it could e.g. analyze the payload (or first bytes) too. 

```scala
val StringFormatConverter: FormatConverter[String] = new FormatConverter[String] {
  override def fits(d: Delivery): Boolean = d.properties.contentType.contains("text/plain")

  override def convert(d: Delivery): Either[ConversionException, String] = Right(d.body.toStringUtf8)
}
```
