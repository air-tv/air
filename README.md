# Air application core

Local-first Kotlin Multiplatform application state for Air. It consumes the
real Stremio, IPTV, and video contracts through sibling composite builds.

The default repository has no server. A future backend can implement
`MediaSyncSource` without changing UI callers. TV authentication similarly
uses the replaceable `TvAuthGateway` contract for device-code/QR and
username/password flows.

```bash
./gradlew jvmTest jsNodeTest wasmJsNodeTest
```
