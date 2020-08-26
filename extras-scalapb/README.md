# RabbitMQ client extras - ScalaPB

This is an extra module that allows to publish and consume events defined as [Google Protocol Buffers](https://developers.google.com/protocol-buffers/) messages, generated to Scala using [ScalaPB](https://scalapb.github.io/).
```groovy
compile 'com.avast.clients.rabbitmq:rabbitmq-client-extras-scalapb_$scalaVersion:x.x.x'
```

## Converters
* `ScalaPBAsBinaryDeliveryConverter`
* `ScalaPBAsBinaryProductConverter`
* `ScalaPBAsJsonDeliveryConverter`
* `ScalaPBAsJsonProductConverter`

## Consumers
There is `ScalaPBConsumer` consumer that is able to consume both binary and JSON events defined as a Protobuf message.
