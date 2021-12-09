## Migration from 8.x.x to 9.0.x

In general, there are MANY changes under the hood, however, only a few changes in the API. The most relevant changes are:

1. `extras-cactus` module was removed.
2. `StreamedDelivery` has now only new `handleWith` method, forcing the user to correct behavior and enabling much more effective tasks
   processing and canceling.
3. The client supports CorrelationId handling. Producer has `implicit cid: CorrelationId` parameter (with default value, so it's optional)
   and you can easily get the ID from message properties on the consumer side. The CID (on consumer side) is taken from both AMQP properties
   and `X-Correlation-Id` header (where the property has precedence and the header is just a fallback). It's propagated to logs in logging
   context (not in message directly).
4. [`PoisonedMessageHandler`](README.md#poisoned-message-handler) is now a part of the core. It's not wrapping your _action_ anymore,
   however, there exists only a few options, describable in configuration.
5. `DeliveryResult.Republish` now has `isPoisoned` parameter (defaults to `true`) determining whether the PMH (if configured) should count
   the attempt or not.
6. Logging in general was somewhat _tuned_.

As there are only very minor changes in the API between 8 and 9 versions, version 8 won't be supported any more (unless necessary).

Some additional notes, mainly for library developers:

1. The library is still cross-compiled to Scala 2.12 and 2.13.
2. All consumers are now free of boilerplate code - new `ConsumerBase` class is a dependency of all consumers.
3. `DefaultRabbitMQClientFactory` was redesigned and it's a class now.
4. "Live" tests were split to `BasicLiveTest`, `StreamingConsumerLiveTest` and `PoisonedMessageHandlerLiveTest`.

---

The client now (9.0.0) uses circe 0.14.1 (only [extras-circe module](extras-circe)), pureconfig 0.17.1 (
only [pureconfig module](pureconfig)), still cats 2 and cats-effect 2.
