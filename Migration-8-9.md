## Migration from 8.x.x to 9.0.x

1. Release is for Scala 2.13 only.
2. `extras-cactus` module was removed.
3. `StreamedDelivery` has now only new `handleWith` method, forcing the user to correct behavior and enabling much more effective tasks
   processing and canceling.
4. Tasks handling in `StreamingConsumer` has been fixed and better tasks canceling was implemented.
5. Client is now aware of CorrelationId, taking it from both AMQP properties and `X-Correlation-Id` header, propagating to all log messages along the MessageId.
6. Logging in general was somewhat _hardened_.

---

1. API is fully effectful.
    * Init methods return [`cats.effect.Resource`](https://typelevel.org/cats-effect/datatypes/resource.html) for easy resource management.
    * Declaration methods return `F[Unit]`.
1. Library could be configured with case classes - this is the default way.
   See [`RabbitMQConnection.make`](core/src/main/scala/com/avast/clients/rabbitmq/RabbitMQConnection.scala#L73).
1. Configuration from [Lightbend `Config`](https://github.com/lightbend/config) was separated to [`pureconfig`](pureconfig) module. Please
   read the docs. It's behavior remained backward compatible with these exceptions:
    * SSL context is now passed explicitly and is **ignored** in `Config` (see more below).
    * **You have to use `import com.avast.clients.rabbitmq.pureconfig._` to be able to use `fromConfig` method!**
    * **Required structure of config has been changed** - all consumers must be in `consumers` block, producers in `producers` and
      additional declarations in `declarations` blocks. See more info in [`pureconfig module docs`](pureconfig).
    * It's required that the config doesn't contain anything else than configuration of the client. Presence of unknown keys will cause
      failure of configuration parsing.
1. SSL is now configured explicitly in `make`/`fromConfig` method by passing `Option[SSLContext]`. Use `Some(SSLContext.getDefault)` for
   enabling SSL without custom (key|trust)store; it's now disabled by default (`None`)!
1. You are now able to specify network recovery strategy.
1. You are now able to specify timeout log level.
1. Additional declarations: `bindQueue` now has more consistent API. The only change is `bindArguments` were renamed to `arguments`.
1. You are now able to configure custom exchange for republishing.

---

The client now uses circe 0.14.1 (only [extras-circe module](extras-circe)), still cats 2.4 and cats-effect 2.4.
