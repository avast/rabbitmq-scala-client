# RabbitMQ client extras - Protobuf

This is an extra module that allows to publish and consume events defined as [Google Protocol Buffers](https://developers.google.com/protocol-buffers/) messages.
```groovy
compile 'com.avast.clients.rabbitmq:rabbitmq-client-extras-protobuf_$scalaVersion:x.x.x'
```

## Converters
* `ProtobufAsBinaryDeliveryConverter`
* `ProtobufAsBinaryProductConverter`
* `ProtobufAsJsonDeliveryConverter`
* `ProtobufAsJsonProductConverter`

## Consumers
There is `ProtobufConsumer` consumer that is able to consume both binary and JSON events defined as a Protobuf message.
