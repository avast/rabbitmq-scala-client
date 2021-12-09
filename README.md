# RabbitMQ client [![CI](https://github.com/avast/rabbitmq-scala-client/actions/workflows/ci.yml/badge.svg)](https://github.com/avast/rabbitmq-scala-client/actions/workflows/ci.yml) [![Version](https://badgen.net/maven/v/maven-central/com.avast.clients.rabbitmq/rabbitmq-client-api_2.13)](https://repo1.maven.org/maven2/com/avast/clients/rabbitmq/rabbitmq-client-api_2.13/)

This client is Scala wrapper over the standard [RabbitMQ Java client](https://www.rabbitmq.com/java-client.html). Goal of this library is to
simplify basic use cases - to provide FP-oriented API for programmers and to shadow the programmer from an underlying client.

The library is configurable both by case classes (`core` module) and by HOCON/Lightbend `Config` (`pureconfig` module).

The library uses concept of _connection_ and derived _producers_ and _consumers_. Note that the _connection_ shadows you from the underlying
concept of AMQP connection and derived channels - it handles channels automatically according to best practises. Each _producer_ and _
consumer_
can be closed separately while closing _connection_ causes closing all derived channels and all _producers_ and _consumers_.

## Dependency

SBT:
`"com.avast.clients.rabbitmq" %% "rabbitmq-client-core" % "x.x.x"`

Gradle:
`compile 'com.avast.clients.rabbitmq:rabbitmq-client-core_$scalaVersion:x.x.x'`

## Modules

1. [api](api) - Contains only basic traits for consumer etc.
1. [core](core) - Main module. The client, configurable by case classes.
1. [pureconfig](pureconfig) - Module for configuration from [`Config`](https://github.com/lightbend/config).
1. [extras](extras/README.md) - Module with some extra features.
1. [extras-circe](extras-circe/README.md) Allows to publish and consume JSON events,
   using [the circe library](https://circe.github.io/circe/).
1. [extras-protobuf](extras-protobuf/README.md) Allows to publish and consume events defined
   as [Google Protocol Buffers](https://developers.google.com/protocol-buffers/) messages (as both JSON and Protobuf), represented as
   standard Java classes.
1. [extras-scalapb](extras-scalapb/README.md) Allows to publish and consume events defined
   as [Google Protocol Buffers](https://developers.google.com/protocol-buffers/) messages (as both JSON and Protobuf), generated to Scala
   using [ScalaPB](https://scalapb.github.io/).

## Migration

There exists a [migration guide](Migration-6_1-8.md) between versions 6.1.x and 8.0.x.
There exists a [migration guide](Migration-8-9.md) between versions 8.x and 9.0.x.

Please note that configuration from Typesafe/Lightbend config has been moved to [pureconfig module](pureconfig) since 8.x.

## Usage

The API is _finally tagless_ (read more e.g. [here](https://www.beyondthelines.net/programming/introduction-to-tagless-final/)) with
[`cats.effect.Resource`](https://typelevel.org/cats-effect/datatypes/resource.html) which is convenient way how to
[manage resources in your app](https://typelevel.org/cats-effect/tutorial/tutorial.html#acquiring-and-releasing-resources). In addition,
there is a support for [streaming](#streaming-support) with [`fs2.Stream`](https://fs2.io/).

The API uses conversions for both consumer and producer, that means you don't have to work directly with `Bytes` (however you still can if
you want to) and you touch only your business model class which is then (de)serialized using provided converter.

The library uses two types of executors - one is for blocking (IO) operations and the second for callbacks. You _have to_ provide both of
them:

1. Blocking executor as `ExecutorService`
1. Callback executor as `scala.concurrent.ExecutionContext`

The default way is to configure the client with manually provided case classes; see [pureconfig](pureconfig) module for a configuration from
HOCON (Lightbend Config).

This is somewhat minimal setup, using [Monix](https://monix.io/) `Task`:

```scala
import java.util.concurrent.ExecutorService

import cats.effect.Resource
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import javax.net.ssl.SSLContext
import monix.eval._
import monix.execution.Scheduler

implicit val sch: Scheduler = ???
val monitor: Monitor = ???

val blockingExecutor: ExecutorService = ???

val sslContext = SSLContext.getDefault

val connectionConfig = RabbitMQConnectionConfig(
  hosts = List("localhost:5432"),
  name = "MyProductionConnection",
  virtualHost = "/",
  credentials = CredentialsConfig(username = "vogon", password = "jeltz")
)

val consumerConfig = ConsumerConfig(
  name = "MyConsumer",
  queueName = "QueueWithMyEvents",
  bindings = List(
    AutoBindQueueConfig(exchange = AutoBindExchangeConfig(name = "OtherAppExchange"), routingKeys = List("TheEvent"))
  )
)

val producerConfig = ProducerConfig(
  name = "MyProducer",
  exchange = "MyGreatApp"
)

// see https://typelevel.org/cats-effect/tutorial/tutorial.html#acquiring-and-releasing-resources

val rabbitMQProducer: Resource[Task, RabbitMQProducer[Task, Bytes]] = {
  for {
    connection <- RabbitMQConnection.make[Task](connectionConfig, blockingExecutor, Some(sslContext))
    /*
      Here you have created the connection; it's shared for all producers/consumers amongst one RabbitMQ server - they will share a single
      TCP connection but have separated channels.
      If you expect very high load, you can use separate connections for each producer/consumer, however it's usually not needed.
       */

    consumer <- connection.newConsumer[Bytes](consumerConfig, monitor) {
      case delivery: Delivery.Ok[Bytes] =>
        Task.now(DeliveryResult.Ack)

      case _: Delivery.MalformedContent =>
        Task.now(DeliveryResult.Reject)
    }

    producer <- connection.newProducer[Bytes](producerConfig, monitor)
  } yield {
    producer
  }
}
```

### Streaming support

It seems quite natural to process RabbitMQ queue with a streaming app.
[`StreamingRabbitMQConsumer`](api/src/main/scala/com/avast/clients/rabbitmq/api/RabbitMQStreamingConsumer.scala) provides you an
[`fs2.Stream`](https://fs2.io/) through which you can easily process incoming messages in a streaming way.

Notice: Using this functionality requires you to know some basics of [FS2](https://fs2.io/guide.html#overview) library. Please see it's
official guide if you're not familiar with it first.

```scala
// skipping imports and common things, they are the same as in general example above

val consumerConfig = StreamingConsumerConfig( // notice: StreamingConsumerConfig vs. ConsumerConfig
  name = "MyConsumer",
  queueName = "QueueWithMyEvents",
  bindings = List(
    AutoBindQueueConfig(exchange = AutoBindExchangeConfig(name = "OtherAppExchange"), routingKeys = List("TheEvent"))
  )
)

val processMyStream: fs2.Pipe[Task, StreamedDelivery[Task, Bytes], Unit] = { in =>
  in.evalMap(d => d.handleWith(d => Task.now(DeliveryResult.Ack))) // TODO you probably want to do some real stuff here
}

val deliveryStream: Resource[Task, fs2.Stream[Task, Unit]] = {
  for {
    connection <- RabbitMQConnection.make[Task](connectionConfig, blockingExecutor, Some(sslContext))
    streamingConsumer <- connection.newStreamingConsumer[Bytes](consumerConfig, monitor)
  } yield {
    val stream: fs2.Stream[Task, StreamedResult] = streamingConsumer.deliveryStream.through(processMyStream)

    // create resilient (self-restarting) stream; see more information below
    lazy val resilientStream: fs2.Stream[Task, StreamedResult] = stream.handleErrorWith { e =>
      // TODO log the error - something is going wrong!
      resilientStream
    }

    resilientStream
  }
}
```

#### Resilient stream

While you should never ever let the stream fail (handle all your possible errors;
see [Error handling](https://fs2.io/guide.html#error-handling)
section in official docs how the stream can be failed), it's important you're able to recover the stream when it accidentally happens. You
can do that by simply _requesting_ a new stream from the client:

```scala
val stream = streamingConsumer.deliveryStream // get stream from client
  .through(processMyStream) // "run" the stream through your processing logic

val failureCounter: Ref[Task, Int] = ??? // TODO: initialize to max recover count!

lazy val resilientStream: fs2.Stream[Task, Unit] = stream.handleErrorWith { err =>
  // handle the error in stream: recover by calling itself
  // TODO don't forget to add some logging/metrics here!
  fs2.Stream.eval(failureCounter.modify(a => (a - 1, a - 1))).flatMap { attemptsRest =>
    if (attemptsRest < 0) fs2.Stream.raiseError[Task](err) else resilientStream
  }
}

resilientStream
```

or use a prepared extension method:

```scala
import com.avast.clients.rabbitmq._

streamingConsumer.deliveryStream // get stream from client
  .through(processMyStream) // "run" the stream through your processing logic
  .makeResilient(maxErrors = 3) { err =>
    Task.delay {
      // TODO don't forget to add some logging/metrics here!
      ()
    }
  }
```

Please refer to the [official guide](https://fs2.io/guide.html#overview) for understanding more deeply how the recovery of `fs2.Stream`
works.

### Providing converters for producer/consumer

Both the producer and consumer require type argument when creating from _connection_:

1. `connection.newConsumer[MyClass]` which requires implicit `DeliveryConverter[MyClass]`
1. `connection.newProducer[MyClass]` which requires implicit `ProductConverter[MyClass]`

There are multiple options where to get the _converter_ (it's the same case for `DeliveryConverter` as for `ProductConverter`):

1. Implement your own implicit _converter_ for the type
1. Modules [extras-circe](extras-circe/README.md) and [extras-scalapb](extras-scalapb/README.md) provide support for JSON and GPB
   conversion.
1. Use `identity` converter by specifying `Bytes` type argument. No further action needed in that case.

### Poisoned message handler

It's quite often use-case we want to republish failed message but want to avoid the message to be republishing forever. You can use
the `PoisonedMessageHandler` to solve this issue. It will count no. of attempts and won't let the message be republished again and again (
above the limit you set).  
_Note: it works ONLY for `Republish` and not for `Retry`!_

The `PoisonedMessageHandler` is built into the both "normal" and streaming consumers. After the execution of the poisoned-message action,
the delivery is REJECTed (so it's not in the original queue anymore).

All types (except no-op) of the poisoned message handler has `maxAttempts` configuration option which determines how many times the message
can be delivered to the consumer. What it means in practice is that if `maxAttempts == 3` and you choose to republish it for the third time,
the PMH takes its action - as the next delivery of the message would be already fourth, which is over the configured limit.

Internally, the attempts counting is done via incrementing (or adding, the first time)  the `X-Republish-Count` header in the message. Feel
free to use its value for your own logging or whatever. You can even set it to your own value - just bear in mind that you might affect the
PMH's functionality (of course, that might be your intention).

It can happen that you know that you have PMH configured and you need to republish the message and "not count the attempt". There exists the
new `isPoisoned` parameter now (defaults to `true`) determining whether the PMH (if configured) should count the attempt or not. This is an
easy and clean way how to influence the PMH behavior.

#### Dead-queue poisoned message handler

The most common and useful type, which will take all "poisoned" messages and publish them to a queue of your choice.  
In its configuration, you basically configure a _producer_ which is used to send the message into the dead-queue. While the producer will
create its exchange it publishes to, *you are responsible for creating the queue and binding* to the producer's exchange. You can use the
[additional declaration/bindings functionality](#additional-declarations-and-bindings) of the client. If you forget to do so, your messages
will be lost completely.

#### Logging poisoned message handler

As the name suggests, this PMH only logs the poisoned message before it's thrown away (and lost forever).

#### No-op poisoned message handler

This PMH does nothing.

#### Example HOCON configuration

Please mind that the PMH is *only* responsible for publishing the poisoned messages, not for declaring/binding the queue where they'll end!

```hocon
myConsumer {
  name = "MyVeryImportantConsumer"

  // ...
  // the usual stuff for consumer - timeout, bindings, ...
  // ...

  poisonedMessageHandling {
    type = "deadQueue" // deadqueue, logging, noop (default noop)

    maxAttempts = 2 // <-- required for deadqueue and logging types

    // required only for deadQueue type:
    deadQueueProducer {
      routingKey = "dead"
      name = "DeadQueueProducer"
      exchange = "EXCHANGE3"
      declare {
        enabled = true
        type = "direct"
      }
    }
  }
}
```

### Caveats

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
   Notice the results of last three calls **differ** even though they are supposed to be the same (non-null respectively)! A very similar
   issue is discussed on the [StackOverflow](https://github.com/circe/circe/issues/380) and so is similar the solution:
    1. Remove explicit type completely (not recommended)
    1. Make the explicit type more general (`DeliveryConverter` instead of `JsonDeliveryConverter` in this case)

## Notes

### Extras

There is a module with some optional functionality called [extras](extras/README.md).

### Network recovery

The library offers configurable network recovery, with the functionality itself backed by RabbitMQ client's one (ready in 5+).  
You can either disable the recovery or select (and configure one of following types):

1. Linear  
   The client will wait `initialDelay` for first recovery attempt and if it fails, will try it again each `period` until it succeeds.
1. Exponential  
   The client will wait `initialDelay` for first recovery attempt and if it fails, will try it again until it succeeds and prolong the delay
   between each two attempts exponentially (based on `period`, `factor`, attempt number), up to `maxLength`.  
   Example:  
   For `initialDelay = 3s, period = 2s, factor = 2.0, maxLength = 1 minute`, produced delays will be 3, 2, 4, 8, 16, 32, 60 seconds
   (and it will never go higher).

Do not set too short custom recovery delay intervals (less than 2 seconds) as it is not recommended by the
official [RabbitMQ API Guide](https://www.rabbitmq.com/api-guide.html#automatic-recovery-limitations).

### DeliveryResult

The consumers `readAction` returns `Future` of [`DeliveryResult`](api/src/main/scala/com/avast/clients/rabbitmq/api/DeliveryResult.scala).
The `DeliveryResult` has 4 possible values
(descriptions of usual use-cases):

1. Ack - the message was processed; it will be removed from the queue
1. Reject - the message is corrupted or for some other reason we don't want to see it again; it will be removed from the queue
1. Retry - the message couldn't be processed at this moment (unreachable 3rd party services?); it will be requeued (inserted on the top of
   the queue)
1. Republish - the message may be corrupted but we're not sure; it will be re-published to the bottom of the queue (as a new message and the
   original one will be removed). It's usually wise to prevent an infinite republishing of the message -
   see [Poisoned message handler](extras/README.md#poisoned-message-handler).

#### Difference between _Retry_ and _Republish_

When using _Retry_ the message can effectively cause starvation of other messages in the queue until the message itself can be processed; on
the other hand _Republish_ inserts the message to the original queue as a new message and it lets the consumer handle other messages (if
they can be processed).

#### Republishing

Republishing is solved at application level with publishing a new message (with original content, headers, messageId, etc.) to the original
queue and acknowledging the old one. This can be done via:

1. Default exchange Every virtual host in RabbitMQ has default exchange which has implicit bindings to all queues and can be easily used for
   publishing to basically any queue. This is very handy for functionality such as the republishing however it's also very dangerous and you
   don't have permissions to use it. In case you do have them, use this option instead of the custom exchange.  
   This the default option (in other words, the client will use the default exchange in case you don't tell it not to do so).
1. Custom exchange In case you're unable to use the default exchange, you have to create your own exchange to replace the functionality. The
   RabbitMQ client will create it for you together with all necessary bindings and all you have to do is to just configure a name of the
   exchange, e.g.
    ```hocon
       rabbitConnection {
         hosts = ["localhost:5672"]
         virtualHost = "/"
       
         ...
       
         republishStrategy {
           type = CustomExchange
       
           exchangeName = "ExchangeForRepublishing"

           exchangeDeclare = true // default
           exchangeAutoBind = true // default
         }
         
         ...
       }
    ```
   The exchange is created as _direct_, _durable_ and without _auto-delete_ flag.

### Bind/declare arguments

There is an option to specify bind/declare arguments for queues/exchanges as you may read about
at [RabbitMQ docs](https://www.rabbitmq.com/queues.html).  
Example of configuration with HOCON:

```hocon
  producer {
  name = "Testing" // this is used for logging etc.

  exchange = "myclient"

  // should the producer declare exchange he wants to send to?
  declare {
    enabled = true // disabled by default

    type = "direct" // fanout, topic

    arguments = {"x-max-length": 10000}
  }
}

```

### Additional declarations and bindings

Sometimes it's necessary to declare an additional queue or exchange which is not directly related to the consumers or producers you have in
your application (e.g. dead-letter queue).  
The library makes possible to do such thing. Here is example of such configuration with HOCON:

```scala
val rabbitConnection: ConfigRabbitMQConnection[F] = ???

rabbitConnection.bindExchange("backupExchangeBinding") // : F[Unit]
```

where the "backupExchangeBinding" is link to the configuration (use relative path to the `declarations` block in configuration):

```hocon
  declarations {
  backupExchangeBinding {
    sourceExchangeName = "mainExchange"
    destExchangeName = "backupExchange"
    routingKeys = ["myMessage"]
    arguments {}
  }
}
```

Equivalent code with using case classes configuration:

```scala
val rabbitConnection: RabbitMQConnection[F] = ???

rabbitConnection.bindExchange(
  BindExchangeConfig(
    sourceExchangeName = "mainExchange",
    destExchangeName = "backupExchange",
    routingKeys = List("myMessage")
  )
) // : F[Unit]
```

### Correlation-ID handling

### Pull consumer

Sometimes your use-case just doesn't fit the _normal_ consumer scenario. Here you can use the _pull consumer_ which gives you much more
control over the received messages. You _pull_ new message from the queue and acknowledge (reject, ...) it somewhere in the future.

The pull consumer uses `PullResult` as return type:

* Ok - contains `DeliveryWithHandle` instance
* EmptyQueue - there was no message in the queue available

Additionally you can call `.toOption` method on the `PullResult`.

A simplified example, using configuration from HOCON:

```scala
import cats.effect.Resource
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.pureconfig._
import com.avast.clients.rabbitmq.api._
import monix.eval.Task
import monix.execution.Scheduler

implicit val sch: Scheduler = ???

val consumer: Resource[Task, RabbitMQPullConsumer[Task, Bytes]] = {
  for {
    connection <- RabbitMQConnection.fromConfig[Task](???, ???)
    consumer <- connection.newPullConsumer[Bytes](??? : String, ???)
  } yield {
    consumer
  }
}

val program: Task[Unit] = consumer.use { consumer =>
  Task
    .sequence {
      (1 to 100).map(_ => consumer.pull())
    } // receive "up to" 100 deliveries
    .flatMap { ds =>
      // do your stuff!

      Task.unit
    }
}
```

### MultiFormatConsumer

Quite often you receive a single type of message but you want to support multiple formats of encoding (Protobuf, Json, ...). This is
where `MultiFormatConsumer` could be used.

Modules [extras-circe](extras-circe/README.md) and [extras-scalapb](extras-scalapb/README.md) provide support for JSON and GPB conversion.
They are both used in the example below.

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
import scala.jdk.CollectionConverters._

private implicit val d: Decoder[Bytes] = Decoder.decodeString.map(???)

case class FileSource(fileId: Bytes, source: String)

case class NewFileSourceAdded(fileSources: Seq[FileSource])

val consumer = MultiFormatConsumer.forType[Future, NewFileSourceAdded](
  JsonDeliveryConverter.derive(), // requires implicit `io.circe.Decoder[NewFileSourceAdded]`
  GpbDeliveryConverter[NewFileSourceAddedGpb]
    .derive() // requires implicit `com.avast.cactus.Converter[NewFileSourceAddedGpb, NewFileSourceAdded]`
)(_ => ???)
```

(see [unit test](core/src/test/scala/com/avast/clients/rabbitmq/MultiFormatConsumerTest.scala) for full example)

#### Implementing own `DeliveryConverter`

The [CheckedDeliveryConverter](core/src/main/scala/com/avast/clients/rabbitmq/converters.scala) is usually reacting to Content-Type (like in
the example below) but it's not required - it could e.g. analyze the payload (or first bytes) too.

```scala
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.CheckedDeliveryConverter
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery}

val StringDeliveryConverter: CheckedDeliveryConverter[String] = new CheckedDeliveryConverter[String] {
  override def canConvert(d: Delivery[Bytes]): Boolean = d.properties.contentType.contains("text/plain")

  override def convert(b: Bytes): Either[ConversionException, String] = Right(b.toStringUtf8)
}
```
