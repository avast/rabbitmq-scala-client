## Migration from 6.1.x to 7.0.x

Common changes:

1. Additional declarations: `bindQueue` now has more consistent API. The only change is `bindArguments` were renamed to `arguments`.
1. You are able to specify network recovery strategy.
1. You are able to specify timeout log level.

Changes in Scala API:

1. API is fully effectful.
    * Init methods return [`cats.effect.Resource`](https://typelevel.org/cats-effect/datatypes/resource.html) for easy resource management.
    * Declaration methods return `F[Unit]`.

Changes in Java API.:

1. You can specify `timeout` in the connection builder. It's used during initialization and closing of the client.

---

The client now uses circe 0.11, Monix 3.0 RC2, cats 1+ and cats-effect 1+.
