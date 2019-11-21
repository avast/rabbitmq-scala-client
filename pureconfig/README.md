# RabbitMQ client - configuration from `Config`

This module contains an alternative way how to configure RabbitMQ client - with [`Config`](https://github.com/lightbend/config) instance.

Before version 8, configuration from `Config` was the only way how to get instance of this client. Since then, configuration from `Config`
is only available with this module. The implementation is one Pureconfig library and the module exposes `ConfigReader` instances for all
configuration classes.

## Dependency
SBT:
`'com.avast.clients.rabbitmq' %% 'rabbitmq-client-pureconfig' % 'x.x.x'`

Gradle:
`compile 'com.avast.clients.rabbitmq:rabbitmq-client-pureconfig_$scalaVersion:x.x.x'`

## Usage

To be able to use the `RabbitMQConnection.fromConfig` method add this to your code:
```scala
import com.avast.clients.rabbitmq.pureconfig._
```

Once you use the `RabbitMQConnection.fromConfig[F]` method, you get an instance of `ConfigRabbitMQConnection[F]` through which you can create
your consumers and producers - the same way as in [core](..) module but with configuration preloaded from passed `Config`.

By default the configuration is checked to hold the exact required format thus it's forbidding any _unknown elements_. This was done to reduce
misconfigurations caused by typos, migrations from older versions etc. It's possible (however not recommended) to turn this check off - see
[related section](#turning-of-strict-format-checking).

### Structured config

This is required format for configuration of a connection and derived consumers etc. _Please note this format has been slightly changed in v8._

```hocon
rabbitConfig {
  // connection config
  
  consumers {
    consumer1 {
      //consumer config
    }
    
    consumer2 {
      //consumer config
    }
  }

  producers {
    producer1 {
      //producer config
    }
    
    producer2 {
      //producer config
    }
  }
  
  declarations {
    bindMyAnotherQueue {
      // bindQueue config
    } 
  }
}

```

#### Config example

```hocon
myConfig {
  hosts = ["localhost:5672"]
  virtualHost = "/"
  
  name = "Cluster01Connection" // used for logging AND is also visible in client properties in RabbitMQ management console

  credentials {
    //enabled = true // enabled by default

    username = "guest"
    password = "guest"
  }

  connectionTimeout = 5s

  networkRecovery {
    enabled = true // enabled by default
    
    handler {
      type = "exponential" // exponential, linear

      initialDelay = 10s
      period = 2s
      maxLength = 32s
    }
  }


  // CONSUMERS AND PRODUCERS:

  consumers {
    // this is the name you use while creating; it's recommended to use something more expressive, like "licensesConsumer"
    testing {
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
  }

  producers {
    // this is the name you use while creating; it's recommended to use something more expressive, like "licensesProducer"
    testing {
      name = "Testing" // this is used for metrics, logging etc.
  
      exchange = "myclient"
  
      // should the producer declare exchange he wants to send to?
      declare {
        enabled = true // disabled by default
  
        type = "direct" // fanout, topic
        durable = true // default value
        autoDelete = false // default value
      }
      
      // These properties are used when none properties are passed to the send method.
      properties {
        deliveryMode = 2 // this is default value
        contentType = "text" // default is not set
        contentEncoding = "UTF8" // default is not set
        priority = 1 // default is not set
      }
    }
  }
}
```
Checkout [configuration caseclasses](../core/src/main/scala/com/avast/clients/rabbitmq/configuration.scala) for all possible values along
with their defaults.


### Scala example

The example below uses [Monix](https://monix.io/) `Task` however it works with all `F[_] : Effect` (`Task`, Cats-Effect `IO`, `ZIO`, ..?).

```scala
import java.util.concurrent.ExecutorService

import cats.effect.Resource
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq._
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.typesafe.config.ConfigFactory
import monix.eval._
import monix.execution.Scheduler

import com.avast.clients.rabbitmq.pureconfig._ // <-- this is needed in order to be able to use `fromConfig` method!

val config = ConfigFactory.load().getConfig("myConfig")

implicit val sch: Scheduler = ??? // required by Task
val monitor: Monitor = ???

val blockingExecutor: ExecutorService = ???

// see https://typelevel.org/cats-effect/tutorial/tutorial.html#acquiring-and-releasing-resources

val rabbitMQProducer: Resource[Task, RabbitMQProducer[Task, Bytes]] = {
    for {
      connection <- RabbitMQConnection.fromConfig[Task](config, blockingExecutor)
      /*
        Here you have created the connection; it's shared for all producers/consumers amongst one RabbitMQ server - they will share a single
        TCP connection but have separated channels.
        If you expect very high load, you can use separate connections for each producer/consumer, however it's usually not needed.
       */

      consumer <- connection.newConsumer[Bytes]("testing", monitor) {
        case delivery: Delivery.Ok[Bytes] =>
          Task.now(DeliveryResult.Ack)

        case _: Delivery.MalformedContent =>
          Task.now(DeliveryResult.Reject)
      }

      producer <- connection.newProducer[Bytes]("testing", monitor)
    } yield {
      producer
    }
  }
```

### Different casing support

The default casing for HOCON keys is `camelCase` but it's also possible to use other ones supported by Pureconfig by using:
```scala
import com.avast.clients.rabbitmq.pureconfig.implicits.KebabCase._
```

See all possible values in `com.avast.clients.rabbitmq.pureconfig.implicits`.

### Turning of strict format checking

It's possible to turn off the check of configuration format. You can do that by importing:
```scala
import com.avast.clients.rabbitmq.pureconfig.implicits.nonStrict._
```

Given the case you want to turn off the check and also change your casing, use following:
```scala
import com.avast.clients.rabbitmq.pureconfig.implicits.nonStrict.KebabCase._
```

