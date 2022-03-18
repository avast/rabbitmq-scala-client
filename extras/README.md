# RabbitMQ client extras

This is an extra module with some optional functionality.

```groovy
compile 'com.avast.clients.rabbitmq:rabbitmq-client-extras_$scalaVersion:x.x.x'
```

## HealthCheck

The library is not able to recover from all failures so it
provides [HealthCheck class](src/main/scala/com/avast/clients/rabbitmq/extras/HealthCheck.scala)
that indicates if the application is OK or not - then it should be restarted. To use that class, simply pass the `rabbitExceptionHandler`
field as listener when constructing the RabbitMQ classes. Then you can call `getStatus` method.
