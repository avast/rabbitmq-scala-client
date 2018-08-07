## Migration from 6.0.x to 6.1.x

Removed Kluzo functionality.

Changes in Scala API:

1. API doesn't require `FromTask`/`ToTask` instance; requires `Effect[F]` instead - not possible to use `Future` directly, see
[Using own non-Efect F](README.md#using-own-non-effect-f)
1. The API of 6.1.x now requires an implicit `ExecutionContext`
