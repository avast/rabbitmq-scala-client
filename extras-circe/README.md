# RabbitMQ client extras - Circe

This is an extra module with some optional functionality dependent on [Circe](https://github.com/circe/circe) (library for decoding JSON into
Scala case classes).  
```groovy
compile 'com.avast.clients:rabbitmq-client-extras-circe_?:x.x.x'
```

## JsonFormatConverter

This is an implementation for [FormatConverter](../core/src/main/scala/com/avast/clients/rabbitmq/FormatConverter.scala) which adds support
for JSON decoding done by [Circe](https://github.com/circe/circe).

The suitability of the converter for concrete message is decided based on Content-Type property - `application/json` is supported.

See [MultiTypeConsumer](../README.md#multitypeconsumer) description for usage.