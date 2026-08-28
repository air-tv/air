# Air application core guidance

- This repository owns shared application state and use cases. Protocol parsing belongs in `stremio-addon-client`/`iptv`; playback engines belong in `video`; Compose UI belongs in platform applications/design.
- The default data path is local-only. `LocalFirstMediaRepository` may accept a future `MediaSyncSource`, but ordinary construction must never require a server.
- Household profiles and settings live in `LocalFirstHouseholdRepository`; keep profile playback/language choices separate from device OLED/motion/decoder settings, and keep current-profile reselection a no-op so TV navigation cannot trigger needless state churn.
- Store and expose the exact normalized contracts from sibling libraries. Do not create parallel UI-shaped copies of Stremio or IPTV protocol data here.
- Authentication supports device-code/QR and username/password through `TvAuthGateway`. The gateway is replaceable; this repository never embeds a server URL or vendor SDK.
- Device codes, passwords, tokens, QR payloads, provider URLs, and playback headers must be redacted from `toString`, logs, analytics, and errors.
- Serialize auth state transitions, enforce device-code expiry before polling, and always rethrow `CancellationException`; cancellation is lifecycle control, not an authentication failure.
- Public async work is suspend; ongoing state is `StateFlow`. Keep constructors small and avoid DI frameworks.
- JDK 17 and Kotlin 2.1.10 are canonical. Run `./gradlew jvmTest jsNodeTest wasmJsNodeTest` for portable changes.
