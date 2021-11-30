## Migration from 8.x.x to 9.0.x

1. Release is for Scala 2.13 only.
2. `extras-cactus` module was removed.
3. `StreamedDelivery` has now only new `handleWith` method, forcing the user to correct behavior and enabling much more effective tasks
   processing and canceling.
4. There is a new concept of _[middlewares](../README.md#consumer-middlewares)_.
5. [`PoisonedMessageHandler`](extras#poisoned-message-handler) (as well as its streaming counterpart) is not wrapping your action anymore.
   It's a _middleware_ instead.
6. Client is now aware of CorrelationId, taking it from both AMQP properties and `X-Correlation-Id` header (where the property has precedence
   and the header is just a fallback), propagating to all log messages along the MessageId.
7. Logging in general was somewhat _tuned_.

---

The client now uses circe 0.14.1 (only [extras-circe module](extras-circe)), still cats 2.4 and cats-effect 2.4.
