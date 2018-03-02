# RabbitMQ client extras - Cactus

This is an extra module with some optional functionality dependent on [Cactus](https://github.com/avast/cactus) (library for converting
between GPBs and Scala case classes).  
```groovy
compile 'com.avast.clients:rabbitmq-client-extras-cactus?:x.x.x'
```

## GpbFormatConverter

This is an implementation for [FormatConverter](../core/src/main/scala/com/avast/clients/rabbitmq/FormatConverter.scala) which adds support
for JSON decoding done by [Circe](https://github.com/circe/circe).

The suitability of the converter for concrete message is decided based on Content-Type property - `application/protobuf` and
`application/x-protobuf` are supported.

See [MultiTypeConsumer](../README.md#multitypeconsumer) description for usage.