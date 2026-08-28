# Air application core

Local-first Kotlin Multiplatform application state for Air. Source composites
are the temporary checked-in default until explicit KMP package releases are
selected. This default is explicit in `gradle.properties`; Gradle never enables
a composite merely because a sibling directory exists.

Package mode has no implicit or checked-in release versions. It requires an
explicit stable `MAJOR.MINOR.PATCH` version for each Stremio, IPTV, and video
contract. GitHub Packages requires authentication even when these packages and
repositories are public.

For package consumption, provide credentials outside the repository through
`GITHUB_ACTOR`/`GITHUB_TOKEN`, or through these entries in the user-level
`~/.gradle/gradle.properties` file:

```properties
githubPackagesUser=YOUR_GITHUB_LOGIN
githubPackagesToken=YOUR_READ_PACKAGES_TOKEN
```

Never add those values to this repository. The current source-composite build
requires all three sibling checkouts and runs normally:

```bash
./gradlew jvmTest --max-workers=2
```

To test released packages, explicitly override the temporary default and name
all three versions:

```bash
./gradlew -PuseLocalAirBuilds=false \
  -PgetAirStremioVersion=X.Y.Z -PgetAirIptvVersion=X.Y.Z \
  -PgetAirVideoVersion=X.Y.Z assertPackageDependencyMode \
  compileCommonMainKotlinMetadata compileKotlinJvm compileKotlinJs \
  compileKotlinWasmJs --max-workers=2
```

CI composite builds explicitly opt in and check out immutable commit SHAs. The
manual `Package Consumer` workflow requires the same three version inputs and
uses the explicit false override. It is intentionally not part of required main
CI until authorized package releases exist.

Required CI proves the package graph without assigning a public release
version. It publishes the immutable sibling refs as `0.0.0-ci` into one isolated
`file://` Maven repository, then compiles Air with composites disabled and all
three versions explicitly set to that fixture version. The non-secret
`getAirPackageRepository` Gradle property (or
`GET_AIR_PACKAGE_REPOSITORY` environment variable) exists only for this local
file-repository gate; ordinary package builds continue to use authenticated
GitHub Packages.

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
native Apple Keychain, native Windows Credential Manager, KDE Wallet from the
Linux JVM target without libsecret, or current-user DPAPI from the Windows JVM
target. Native Windows payloads use generation-addressed chunks so Credential
Manager's small blob limit cannot truncate Stalker fields or request headers.
The Windows Compose/JVM adapter sends payloads only through PowerShell stdin and
atomically stores DPAPI ciphertext under hashed source IDs. Plaintext fallback
is not acceptable. Only metadata has an optional future sync source. Browser
persistence and plaintext production vaults are deliberately not supplied
because browser storage is not an OS credential vault.

`LocalFirstContinueWatchingRepository` keeps resume progress per household
profile using typed Stremio movie/series IDs or IPTV movie/episode IDs. Live TV
cannot enter the model. Completed items leave the shelf, each profile is
bounded, and future history sync remains optional.

Household/settings, non-secret source metadata, and Continue Watching can be
opened as versioned persistent JSON documents through one `LocalDocumentStore`.
`JvmFileDocumentStore`, `AndroidFileDocumentStore`, and
`AppleFileDocumentStore` use atomic replacement; JS/Wasm exposes
`browserLocalDocumentStore` for non-secret localStorage documents. A failed
durable write never updates the observable `StateFlow`. Android stores these
documents under the app's no-backup directory. Provider credentials and
configured URLs remain in the separate OS credential vault and never enter
these documents or browser localStorage.

Large media catalogs and TV guides use `DurableCatalogStore`, not observable
whole-guide documents. Refreshes build private source/feed generations and
atomically publish exact counts; failed, cancelled, stale, or crashed writers
leave the previous snapshot readable. `DurableCatalogEpgStore` implements the
IPTV `EpgStore` contract over that storage with bounded batching, rolling
retention, leased cold search projections, revision-bound locators, now/next,
multi-channel windows, conservative channel matching, and bounded matcher
caches. Raw provider source/channel IDs and credential-bearing URLs are hashed
or rejected before persistence.

Android, JVM desktop, and Kotlin/Native use SQLDelight/SQLite with verified
schema migrations and bounded cleanup. Browser JS and Wasm use IndexedDB v4.
Its schema upgrade creates only empty stores; legacy v3 guide timelines migrate
after open in bounded, yielding, crash-resumable batches while reads retain a
correct legacy fallback. IndexedDB is a browser backend—it is not used by the
Android TV application.

Shell setup is one suspend call: `openAndroidLocalApplicationState`,
`openAppleLocalApplicationState`, `openLinuxLocalApplicationState`,
`openWindowsLocalApplicationState`, or `openBrowserLocalApplicationState`.
Each composes durable documents, the platform credential vault, and local-first
repositories without a DI container. Browser source credentials are
intentionally session-only.

```bash
CHROME_BIN=/usr/bin/chromium AIR_SQLITE_LIBRARY_DIR=/usr/lib ./gradlew \
  verifyCommonMainAirCatalogDatabaseMigration \
  jvmTest jsBrowserTest wasmJsBrowserTest linuxX64Test \
  compileDebugKotlinAndroid --max-workers=2
```
