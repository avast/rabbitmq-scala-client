# RabbitMQ client extras

This is an extra module with some optional functionality.  
```groovy
compile 'com.avast.clients:rabbitmq-client-extras_$scalaVersion:x.x.x'
```

## HealthCheck
The library is not able to recover from all failures so it provides [HealthCheck class](src/main/scala/com/avast/clients/rabbitmq/extras/HealthCheck.scala)
 that indicates if the application is OK or not - then it should be restarted.
To use that class, simply pass the `rabbitExceptionHandler` field as listener when constructing the RabbitMQ classes. Then you can call `getStatus` method.

## Poisoned message handler
It's quite often use-case we want to republish failed message but want to avoid the message to be republishing forever. Wrap your handler (readAction)
with [PoisonedMessageHandler](src/main/scala/com/avast/clients/rabbitmq/extras/PoisonedMessageHandler.scala) to solve this issue. It will count no.
of attempts and won't let the message to be republished again and again (above the limit you set).  
_Note: it works ONLY for `Republish` and not for `Retry`!_

The `PoisonedMessageHandler` is _finally tagless_ for Scala (see [related info](../README.md#scala-usage)) and bound to `CompletableFuture` for Java.

Scala:

```scala
val newReadAction = PoisonedMessageHandler[Task, MyDeliveryType](3)(myReadAction)
```

Java:

```java
newReadAction = PoisonedMessageHandler.forJava(3, myReadAction, executor);
```

You can even pretend lower number of attempts when you want to rise the republishing count (for some special message):

```scala
Republish(Map(PoisonedMessageHandler.RepublishCountHeaderName -> 1.asInstanceOf[AnyRef]))
```

Note you can provide your custom poisoned-message handle action:

```scala
val newReadAction = PoisonedMessageHandler.withCustomPoisonedAction[Task, MyDeliveryType](3)(myReadAction) { delivery =>
  logger.warn(s"Delivery $delivery is poisoned!")
  Task.unit
}
```

After the execution of the poisoned-message action (no matter whether default or custom one), the delivery is REJECTed.
