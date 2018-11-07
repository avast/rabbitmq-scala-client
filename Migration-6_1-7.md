## Migration from 6.1.x to 7.0.x

Additional declarations: `bindQueue` now has more consistent API. The only change is `bindArguments` were renamed to `arguments`.

Changes in Scala API:

1. API is fully effectful; all methods return `F[_]` instance (including initialization and closing)

Changes in Java API.:

1. You can specify `initTimeout` in the connection builder.
