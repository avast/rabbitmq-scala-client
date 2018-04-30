# RabbitMQ client extras - Circe

This is an extra module with some optional functionality dependent on [Circe](https://github.com/circe/circe) (library for decoding JSON into
Scala case classes).  
```groovy
compile 'com.avast.clients:rabbitmq-client-extras-circe_$scalaVersion:x.x.x'
```

## JsonDeliveryConverter

This is an implementation of [DeliveryConverter](../core/src/main/scala/com/avast/clients/rabbitmq/DeliveryConverter.scala) which adds support
for JSON decoding done by [Circe](https://github.com/circe/circe).

The suitability of the converter for concrete message is decided based on Content-Type property - `application/json` is supported.

See [Providing converters](../README.md#providing-converters-for-producer/consumer) and [MultiFormatConsumer](../README.md#multiformatconsumer)
description for usage.