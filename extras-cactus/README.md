# RabbitMQ client extras - Cactus

This is an extra module with some optional functionality dependent on [Cactus](https://github.com/avast/cactus) (library for converting
between GPBs and Scala case classes).  
```groovy
compile 'com.avast.clients:rabbitmq-client-extras-cactus_$scalaVersion:x.x.x'
```

## GpbDeliveryConverter

This is an implementation of [DeliveryConverter](../core/src/main/scala/com/avast/clients/rabbitmq/converters.scala) which adds support
for GPB decoding done by [Cactus](https://github.com/avast/cactus).

The suitability of the converter for concrete message is decided based on Content-Type property - `application/protobuf` and
`application/x-protobuf` are supported.

See [Providing converters](../README.md#providing-converters-for-producer/consumer) and [MultiFormatConsumer](../README.md#multiformatconsumer)
description for usage.