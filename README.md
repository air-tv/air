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

`LocalFirstHouseholdRepository` owns the application-shell contract for
household profiles and settings: per-profile playback, live-TV, subtitle/audio
and rating choices plus device-level OLED, motion, density, refresh, decoder,
timeout, response-limit and diagnostics settings. It is local by default and
accepts a `HouseholdSyncSource` only when a future server exists.

`LocalSourceRegistry` stores only non-secret source names/kinds/enabled state in
ordinary local state. Exact `XtreamCredentials`/`StalkerCredentials`, M3U/XMLTV
URLs and headers, and Stremio manifest URLs stay behind a required
`LocalSourceSecretStore`. Production adapters use Android Keystore plus AES-GCM,
native Apple Keychain, native Windows Credential Manager, or KDE Wallet from the
Linux JVM target without libsecret. Windows payloads use generation-addressed
chunks so Credential Manager's small blob limit cannot truncate Stalker fields
or request headers. A Compose/JVM macOS or Windows shell will still need a tiny
native vault bridge; plaintext fallback is not acceptable. Only metadata has an
optional future sync source. Browser persistence and plaintext production vaults
are deliberately not supplied because browser storage is not an OS credential
vault.

`LocalFirstContinueWatchingRepository` keeps resume progress per household
profile using typed Stremio movie/series IDs or IPTV movie/episode IDs. Live TV
cannot enter the model. Completed items leave the shelf, each profile is
bounded, and future history sync remains optional.

```bash
./gradlew jvmTest jsNodeTest wasmJsNodeTest
```
