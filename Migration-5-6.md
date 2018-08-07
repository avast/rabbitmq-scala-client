## Migration from 5.x to 6.x

Common changes:
1. GroupId of all artifacts has changed from `com.avast.clients` to `com.avast.clients.rabbitmq`.
1. The library doesn't use [Lyra library](https://github.com/jhalterman/lyra) anymore. Removing the Lyra resulted into change of listeners.

Changes in Scala API:

1. `RabbitMQFactory` was renamed to `RabbitMQConnection`. It's factory method returns `DefaultRabbitMQConnection` and requires an
`ExecutorService` to be passed (was optional before).
1. The whole API is _finally tagless_ - all methods now return `F[_]`. See [related section](README.md#scala-usage) in docs.
1. The API now uses type-conversions - provide type and related converter when creating producer/consumer.
See [related section](README.md#providing-converters-for-producer/consumer) in docs.
1. The `Delivery` is now sealed trait - there are `Delivery.Ok[A]` (e.g. `Delivery[Bytes]`, depends on type-conversion) and `Delivery.MalformedContent`.
After getting the `Delivery[A]` you should pattern-match it.
1. The API now requires an implicit `monix.execution.Scheduler` instead of `ExecutionContext`.
1. Methods like `RabbitMQConnection.declareQueue` now return `F[Unit]` (was `Try[Done]` before).
1. Possibility to pass manually created configurations (`ProducerConfig` etc.) is now gone. The only option is to use TypeSafe config.
1. There is no `RabbitMQConsumer.bindTo` method anymore. Use [additional declarations](README.md#additional-declarations-and-bindings) for such thing.
1. There are new methods in [`RabbitMQConnection`](core/src/main/scala/com/avast/clients/rabbitmq/RabbitMQConnection.scala): `newChannel` and `withChannel`.
1. [`RabbitMQPullConsumer`](README.md#pull-consumer) was added

Changes in Java API:

1. `RabbitMQFactory` was renamed to `RabbitMQJavaConnection`
1. `RabbitMQJavaConnection.newBuilder` requires an `ExecutorService` to be passed (was optional before)
1. Possibility to pass manually created configurations (`ProducerConfig` etc.) is now gone. The only option is to use TypeSafe config.
1. Methods like `RabbitMQJavaConnection.declareQueue` now return `CompletableFuture[Void]` (was `void` before) - ***it's not blocking anymore!***
1. Method `RabbitMQProducer.send` now returns `CompletableFuture[Void]` (was `void` before) - ***it's not blocking anymore!***
1. `RabbitMQConsumer` and `RabbitMQProducer` (`api` module) are now traits and have their `Default*` counterparts in `core` module
1. There is no `RabbitMQConsumer.bindTo` method anymore. Use [additional declarations](README.md#additional-declarations-and-bindings) for such thing.
1. `RabbitMQPullConsumer` was added
