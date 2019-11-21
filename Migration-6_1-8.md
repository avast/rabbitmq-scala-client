## Migration from 6.1.x to 8.0.x


Java API was **COMPLETELY REMOVED** and won't be supported anymore. Checkout version 7.x for the last version supporting Java API.

---

1. API is fully effectful.
    * Init methods return [`cats.effect.Resource`](https://typelevel.org/cats-effect/datatypes/resource.html) for easy resource management.
    * Declaration methods return `F[Unit]`.
1. Library could be configured with case classes - this is the default way. See [`RabbitMQConnection.make`](core/src/main/scala/com/avast/clients/rabbitmq/RabbitMQConnection.scala#L73).
1. Configuration from [Lightbend `Config`](https://github.com/lightbend/config) was separated to [`pureconfig`](pureconfig) module. It's
behavior remained backward compatible with these exceptions:
    * SSL context is now passed explicitly and is **ignored** in `Config` (see more below).
    * **You have to use `import com.avast.clients.rabbitmq.pureconfig._` to be able to use `fromConfig` method!**
    * **Required structure of config has been changed** - all consumers must be in `consumers` block, producers in `producers` and additional
    declarations in `declarations` blocks. See more info in [`pureconfig module docs`](pureconfig).
    * It's required that the config doesn't contain anything else than configuration of the client. Presence of unknown keys will cause failure
    of configuration parsing. 
1. SSL is now configured explicitly in `make`/`fromConfig` method by passing `Option[SSLContext]`. Use `Some(SSLContext.getDefault)` for enabling
SSL without custom (key|trust)store; it's now disabled by default (`None`)!
1. You are now able to specify network recovery strategy.
1. You are now able to specify timeout log level.
1. Additional declarations: `bindQueue` now has more consistent API. The only change is `bindArguments` were renamed to `arguments`.

---

The client now uses circe 0.11, cats 2+ and cats-effect 2+ (doesn't use Monix anymore).
