## Migration from 6.1.x to 6.2.x

Changes in Scala API:

1. API is fully effectful; all methods return `F[_]` instance (including initialization and closing)

Changes in Java API.:

1. You can specify `initTimeout` in the connection builder.
