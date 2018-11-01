## Migration from 6.1.x to 6.2.x

Changes in Scala API:

1. API is fully effectful; all methods return `F[_]` instance (including initialization and closing)
