# Air application core

Local-first Kotlin Multiplatform application state for Air. It consumes the
real Stremio, IPTV, and video contracts through sibling composite builds.

The default repository has no server. A future backend can implement
`MediaSyncSource` without changing UI callers. TV authentication similarly
uses the replaceable `TvAuthGateway` contract for device-code/QR and
username/password flows.

Auth commands are serialized so rapid remote input cannot race device-code,
password, and sign-out transitions. Device codes are rejected locally at their
expiry instant, and coroutine cancellation always propagates instead of being
rendered as a fake authentication failure.

```bash
./gradlew jvmTest jsNodeTest wasmJsNodeTest
```
