# RabbitMQ client

[![Build Status](https://travis-ci.org/avast/rabbitmq-scala-client.svg?branch=master)](https://travis-ci.org/avast/rabbitmq-scala-client)
[![Download](https://api.bintray.com/packages/avast/maven/rabbitmq-scala-client/images/download.svg) ](https://bintray.com/avast/maven/rabbitmq-scala-client/_latestVersion)


This client is lightweight wrapper over standard [RabbitMQ java client](https://www.rabbitmq.com/java-client.html).
It's API may be difficult to use for inexperienced RabbitMQ users. Goal of this library is to simplify basic use cases and shadow the programmer
from the underlying client.

The library has both Scala and Java API where the Scala API is generic and gives you an option to adapt to your application's type -
see [Scala usage below](#scala-usage).

The library uses concept of _connection_ and derived _producers_ and _consumers_. Note that the _connection_ shadows you from the underlying
concept of AMQP connection and derived channels - it handles channels automatically according to best practises. Each _producer_ and _consumer_
can be closed separately while closing _connection_ causes closing all derived channels and all _producers_ and _consumers_.

## Dependency
`compile 'com.avast.clients.rabbitmq:rabbitmq-client-core_$scalaVersion:x.x.x'`

## Modules

There are [api](api) and [core](core) modules available for the most common usage but there are also few _extras_ modules which contains
some optional functionality:
1. [extras](extras/README.md)
1. [extras-circe](extras-circe/README.md) (adds some circe-dependent functionality)
1. [extras-cactus](extras-cactus/README.md) (adds some cactus-dependent functionality)

## Migration

There is a [migration guide](Migration-5-6.md) between versions 5 and 6.

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
  
  name = "Cluster01Connection" // used for logging AND is also visible in client properties in RabbitMQ management console

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

The Scala API is now _finally tagless_ (read more e.g. [here](https://www.beyondthelines.net/programming/introduction-to-tagless-final/)) -
you can change the type it works with by specifying it when creating the _connection_. In general you have to provide
`cats.arrow.FunctionK[Task, A]` and `cats.arrow.FunctionK[A, Task]` however there are some types supported out-of-the-box by just importing
`import com.avast.clients.rabbitmq._` (`scala.util.Try`, `scala.concurrent.Future` and `monix.eval.Task` currently).

The Scala API uses types-conversions for both consumer and producer, that means you don't have to work directly with `Bytes` (however you
still can, if you want) and you touch only your business class which is then (de)serialized using provided converter.

The library uses two types of executors - one is for blocking (IO) operations and the second for callbacks. You _have to_ provide both of them:
1. Blocking executor as `ExecutorService`
1. Callback executor as `monix.execution.Scheduler` - you can get it e.g. by calling `Scheduler(myFavoriteExecutionContext)`

```scala
import com.typesafe.config.ConfigFactory
import com.avast.metrics.api.Monitor
import com.avast.clients.rabbitmq._ // for generic types support
import com.avast.bytes.Bytes
import monix.execution._
import monix.eval._

val config = ConfigFactory.load().getConfig("myRabbitConfig")

implicit val sch: Scheduler = ???
val blockingExecutor: ExecutorService = Executors.newCachedThreadPool()

val monitor: Monitor = ???

// here you create the connection; it's shared for all producers/consumers amongst one RabbitMQ server - they will share a single TCP connection
// but have separated channels
// if you expect very high load, you can use separate connections for each producer/consumer, but it's usually not needed
val rabbitConnection = RabbitMQConnection.fromConfig[Task](config, blockingExecutor) // DefaultRabbitMQConnection[Task]

val consumer = rabbitConnection.newConsumer[Bytes]("consumer", monitor) { (delivery: Delivery[Bytes]) =>
  println(delivery)
  Task.now(DeliveryResult.Ack)
} // DefaultRabbitMQConsumer

val sender = rabbitConnection.newProducer("producer", monitor) // DefaultRabbitMQProducer[Task]

sender.send(...).runAsync // because it's Task, don't forget to run it ;-)
```

#### Providing converters for producer/consumer

Both the producer and consumer require type argument when creating from _connection_:
1. `connection.newConsumer[MyClass]` which requires implicit `DeliveryConverter[MyClass]`
1. `connection.newProducer[MyClass]` which requires implicit `ProductConverter[MyClass]`

There are multiple options where to get the _converter_ (it's the same case for `DeliveryConverter` as for `ProductConverter`):
1. Implement your own implicit _converter_ for the type
1. Modules [extras-circe](extras-circe/README.md) and [extras-cactus](extras-cactus/README.md) provide support for JSON and GPB conversion. 
1. Use `identity` converter by specifying `Bytes` type argument. No further action needed in that case.

#### Caveats
1. `null` instead of converter instance  
    It may happen you run in this problem:
    ```scala
    scala> import io.circe.generic.auto._
    import io.circe.generic.auto._
    
    scala> import com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter
    import com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter
    
    scala> import com.avast.clients.rabbitmq.DeliveryConverter
    import com.avast.clients.rabbitmq.DeliveryConverter
    
    scala> case class Event(name: String)
    defined class Event
    
    scala> implicit val deliveryConverter: JsonDeliveryConverter[Event] = JsonDeliveryConverter.derive[Event]()
    deliveryConverter: com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter[Event] = null
    
    scala> implicit val deliveryConverter: DeliveryConverter[Event] = JsonDeliveryConverter.derive[Event]()
    deliveryConverter: com.avast.clients.rabbitmq.DeliveryConverter[Event] = com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter$$anon$1@5b977aaa
    
    scala> implicit val deliveryConverter = JsonDeliveryConverter.derive[Event]()
    deliveryConverter: com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter[Event] = com.avast.clients.rabbitmq.extras.format.JsonDeliveryConverter$$anon$1@4b024fb2
    ```
    Notice the results of last three calls **differ** even though they are supposed to be the same (non-null respectively)! A very similar issue
    is discussed on the [StackOverflow](https://github.com/circe/circe/issues/380) and so is similar the solution:
    1. Remove explicit type completely (not recommended)
    1. Make the explicit type more general (`DeliveryConverter` instead of `JsonDeliveryConverter` in this case)

1. Cryptic errors  
    It may happen you have everything configured "correctly" and the compiler still reports an error, for example
    ```scala
    def rabbitConnection(): RabbitMQConnection[Future] = RabbitMQConnection.fromConfig[Future](rabbitProperties, blocking)
    ```
    can give you something like:
    ```
    Error:(22, 99) could not find implicit value for evidence parameter of type com.avast.clients.rabbitmq.FromTask[scala.concurrent.Future]
    Error occurred in an application involving default arguments.
      def rabbitConnection(): RabbitMQConnection[Future] = RabbitMQConnection.fromConfig[Future](rabbitProperties, blocking)
    Error:(22, 99) not enough arguments for method fromConfig: (implicit evidence$3: com.avast.clients.rabbitmq.FromTask[scala.concurrent.Future], implicit evidence$4: com.avast.clients.rabbitmq.ToTask[scala.concurrent.Future])com.avast.clients.rabbitmq.DefaultRabbitMQConnection[scala.concurrent.Future].
    Unspecified value parameters evidence$3, evidence$4.
    Error occurred in an application involving default arguments.
      def rabbitConnection(): RabbitMQConnection[Future] = RabbitMQConnection.fromConfig[Future](rabbitProperties, blocking)
    ```
    This is caused by absence of `ExecutionContext` which makes `def fkToFuture(implicit ec: ExecutionContext): FromTask[Future]` impossible to
    use (unfortunately compiler won't say that).  
    Please bear in mind there is nothing this library could do to help you in this case - there is no way to provide any hint. However there are
    some compiler plugins available which may help you to prevent such situations, e.g. [Splain](https://github.com/tek/splain).

### Java usage

The Java api is placed in subpackage `javaapi` (but not all classes have their Java counterparts, some have to be imported from Scala API,
depending on your usage).  
Don't get confused by the Java API partially implemented in Scala.

```java
import com.avast.clients.rabbitmq.javaapi.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
                ...,
                callbackExecutor,
                ...
        );

        final RabbitMQProducer rabbitMQProducer = connection.newProducer(
                "producer",
                ...,
                callbackExecutor
        );
    }

}
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
    rabbitConnection.bindExchange("backupExchangeBinding")
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

### Pull consumer
Sometimes your use-case just doesn't fit the _normal_ consumer scenario. Here you can use the _pull consumer_ which gives you much more
control over the received messages. You _pull_ new message from the queue and acknowledge (reject, ...) it somewhere in the future.

The pull consumer operates with `Option` which is used for expressing either getting the delivery _or_ detecting an empty queue.

A simplified example:
```scala
import com.avast.bytes.Bytes
import scala.concurrent.Future
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.api._

implicit val sch: Scheduler = ???

val connection: RabbitMQConnection[Future] = ???

val consumer = connection.newPullConsumer[Bytes](???, ???)


// receive "up to" 100 deliveries
val deliveries: Future[Seq[Option[DeliveryWithHandle[Future, Bytes]]]] = Future.sequence { (1 to 100).map(_ => consumer.pull()) }

// do your stuff!

???

// "handle" all deliveries
val handleResult: Future[Unit] = deliveries.flatMap(s => Future.sequence(s.flatten.map(_.handle(DeliveryResult.Ack))).map(_ => Unit))

consumer.close()
connection.close()

```

### MultiFormatConsumer

Quite often you receive a single type of message but you want to support multiple formats of encoding (Protobuf, Json, ...).
This is where `MultiFormatConsumer` could be used.  

Modules [extras-circe](extras-circe/README.md) and [extras-cactus](extras-cactus/README.md) provide support for JSON and GPB conversion. They
are both used in the example below.

The `MultiFormatConsumer` is Scala only.

Usage example:

[Proto file](core/src/test/proto/ExampleEvents.proto)

```scala
import com.avast.bytes.Bytes
import com.avast.cactus.bytes._ // Cactus support for Bytes, see https://github.com/avast/cactus#bytes
import com.avast.clients.rabbitmq.test.ExampleEvents.{NewFileSourceAdded => NewFileSourceAddedGpb}
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.extras.format._
import io.circe.Decoder
import io.circe.generic.auto._ // to auto derive `io.circe.Decoder[A]` with https://circe.github.io/circe/codec.html#fully-automatic-derivation
import scala.concurrent.Future
import scala.collection.JavaConverters._

private implicit val d: Decoder[Bytes] = Decoder.decodeString.map(Utils.hexToBytesImmutable)

case class FileSource(fileId: Bytes, source: String)

case class NewFileSourceAdded(fileSources: Seq[FileSource])

val consumer = MultiFormatConsumer.forType[Future, NewFileSourceAdded](
  JsonDeliveryConverter.derive(), // requires implicit `io.circe.Decoder[NewFileSourceAdded]`
  GpbDeliveryConverter[NewFileSourceAddedGpb].derive() // requires implicit `com.avast.cactus.Converter[NewFileSourceAddedGpb, NewFileSourceAdded]`
)(
  businessLogic.processMessage,
  failureHandler
)
```
(see [unit test](core/src/test/scala/com/avast/clients/rabbitmq/MultiFormatConsumerTest.scala) for full example)

#### Implementing own `DeliveryConverter`

The [CheckedDeliveryConverter](core/src/main/scala/com/avast/clients/rabbitmq/converters.scala) is usually reacting to Content-Type (like in
the example below) but it's not required - it could e.g. analyze the payload (or first bytes) too. 

```scala
val StringDeliveryConverter: CheckedDeliveryConverter[String] = new CheckedDeliveryConverter[String] {
  override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.contains("text/plain")
  override def convert(b: Bytes): Either[ConversionException, String] = Right(b.toStringUtf8)
}
```
