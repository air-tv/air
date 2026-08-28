# ADR-0002: Production application shells and canonical Compose UI ownership

- Status: Proposed for implementation; no production repository has been created
- Date: 2026-08-28
- Owners: Air application and platform shells

## Context

Air needs executable Android TV, Android phone/tablet, JVM desktop, iOS, and
browser applications without turning the `design` repository into the product.
The existing design executable is valuable because its Android TV focus,
navigation, player chrome, and screen layouts have already been exercised in the
canonical TV emulator. It must remain the visual and interaction harness, but a
production application must not depend on `com.getair.design`, `StaticData`, a
test asset, or an Android activity owned by that repository.

The current repositories establish these facts:

- `design` is one Android application module. `AirTvDesignApp` owns routing and
  mutable mock state; several screens still read `StaticData` directly.
- The design models wrap the real public `air`, `iptv`, and
  `stremio-addon-client` contracts. They are not independent copies of those
  protocol models.
- `PlayerScreen` creates `AndroidMedia3BackendFactory` itself, attaches a
  `TextureView`, and opens a bundled test movie. This is deliberately useful in
  the harness and deliberately unsuitable as production composition.
- Most TV widgets use `androidx.tv.material3`. That library is Android-specific,
  so the exact TV implementation cannot honestly be moved to `commonMain` and
  claimed to be a desktop, Apple, or web UI.
- `air` already exposes local-first household, source, continue-watching, and
  profile-library repositories through platform state factories. It also has
  SQLDelight catalog factories for Android, JVM, and Native.
- `video` is the backend-neutral player contract. Android Media3, Apple
  AVFoundation, browser HTML video, and JVM backends live behind it. The
  optional `mediamp-air` JVM artifact supplies the movable Compose MPV surface
  for desktop without exposing mediamp types to the application.
- At this audit snapshot, direct IndexedDB catalog storage is concurrently in
  progress but is not yet merged and verified. Browser source secrets are
  intentionally session-only. A JVM macOS Keychain-backed source-secret factory
  is also absent; the existing Apple Keychain implementation is Kotlin/Native
  and cannot be reused by a JVM desktop process.
- Local development currently uses sibling Gradle composite builds. IPTV,
  Stremio, Vizio, and video have GitHub Packages release workflows; `air` does
  not yet publish a KMP artifact. The mediamp Air package workflow exists but
  remains unproven until an authorized immutable release.

Compose Multiplatform's current official project structure uses a shared KMP
module plus thin Android, desktop, web, and Xcode iOS application entry points
([project structure](https://github.com/JetBrains/kotlin-multiplatform-dev-docs/blob/master/topics/compose-onboard/compose-multiplatform-create-first-app.md)).
The current desktop packaging API remains `compose.desktop.application` with
`nativeDistributions`, and an iOS native view can be hosted with `UIKitView`
([desktop packaging](https://github.com/JetBrains/compose-multiplatform/blob/master/tutorials/Signing_and_notarization_on_macOS/README.md),
[UIKit interop](https://github.com/JetBrains/compose-multiplatform/blob/master/examples/interop/ios-uikit-in-compose/README.md)).
The current web entry still requires an executable browser target. Current KMP
templates also provide platform UI-test source sets rather than treating a
common compile as UI verification.

Those API shapes are accepted here. The latest documentation examples use a
newer Kotlin/Compose toolchain than Air's fixed Kotlin 2.1.10 baseline. This ADR
does not silently import those example versions. The implementation must pin a
Compose Multiplatform version that compiles under Kotlin 2.1.10 and prove the
full target matrix before changing the workspace compiler baseline.

## Decision

Create one production application repository, provisionally `get-air/app`,
only when implementation begins. Keep the minimum justified structure:

```text
app/
  shared/          KMP production composition, presentation state, and UI
  androidApp/      one Android application for TV and phone/tablet
  desktopApp/      JVM Compose application for Linux, Windows, and macOS
  webApp/          Wasm browser executable; JS may remain a compatibility target
  iosApp/          Xcode project consuming the shared iOS framework
```

Do not create `data`, `domain`, `usecases`, `navigation`, `di`, `player`, or
per-screen Gradle modules. The existing libraries are already the domain and
backend boundaries. Add another module only when it has an independently
testable runtime boundary or an independently shipped artifact.

The first executable slice creates only `shared` and `androidApp`. Desktop,
web, and iOS entry modules land with their first runnable shell, not as empty
scaffolds.

### Repository ownership

| Owner | Responsibility | Explicitly does not own |
| --- | --- | --- |
| `air` | local-first application state, profile/source policy, durable catalog contract and platform storage factories, bounded refresh/tune/connection coordination | Compose screens, platform entry points, source transport orchestration |
| `iptv` | Xtream, Stalker, M3U/XMLTV parsing, normalized models, URL/link resolution | household policy, UI, persisted credentials |
| `stremio-addon-client` | addon discovery/resources/limits and exact Stremio models | addon assignment policy, UI |
| `video` | backend-neutral playback contract and platform engines | application chrome, routes, source selection |
| `mediamp` | optional bundled desktop MPV implementation and Compose surface | Air presentation or application state |
| proposed `app` | canonical presentation models, production UI source, composition root, platform entry points, packaging | protocol reimplementation, generic framework layers |
| `design` | static fixtures, visual scenarios, screenshot/focus/performance harness | production state, transports, persistence, release application |

No production module may depend on `design`. `design` will instead depend on
the canonical TV UI owned by `app:shared`.

### Extract the exact design UI once

Avoid both alternatives that create permanent drift: do not copy the screens
into production and leave the originals, and do not make the design APK the
production APK.

Perform one source-ownership migration:

1. Move `Models.kt` and route/presentation contracts into
   `app/shared/src/commonMain`. Keep `ArtworkPalette` as a UI value and keep the
   existing `Meta`, `IptvChannel`, `IptvMovie`, `IptvSeriesDetails`, and
   `EpgProgramme` fields. Do not invent application copies of those models.
2. Move the exact Android TV components, theme, focus helpers, and screens into
   `app/shared/src/androidMain`. They remain based on Android TV Material and
   retain the tested focus behavior.
3. Remove all `StaticData` references from canonical screen source. Supply one
   immutable TV presentation snapshot and actions at the root. Specifically,
   inject featured items, continue watching, live channels, movie and series
   pages, profiles and palettes, settings/source state, selected item, and
   player state. No leaf screen reaches into a singleton repository.
4. Keep `StaticData`, the exact protocol-shaped mock fixtures, the bundled test
   media, and the design `MainActivity` in `design`. Its root adapts those
   fixtures to the same production UI contract.
5. Delete the migrated screen/model files from `design` in the same change that
   makes its application consume `app:shared`. There is one canonical source,
   not a copied implementation.

The extraction gate is visual and behavioral equivalence, not a redesign. The
design APK must retain its existing routes and emulator focus tests. Phone,
desktop, iOS, and web may compose layouts appropriate to their inputs, but they
must reuse the same presentation models, actions, protocol objects, player
chrome state, and shared primitives instead of cloning the TV screen models.
Android TV Material code stays Android-specific rather than being disguised
behind fake common wrappers.

The common UI boundary should be small:

```kotlin
data class AirUiSnapshot(/* bounded screen windows and selected profile */)

sealed interface AirUiIntent {
    // navigation, selection, profile/source/settings and playback intents
}

@Composable
fun AirApp(
    snapshot: AirUiSnapshot,
    dispatch: (AirUiIntent) -> Unit,
    playerHost: PlayerUiHost,
)
```

This is one state/intent surface, not a parallel callback, effect, and command
framework. The exact names may change during extraction, but these invariants
do not:

- UI receives immutable, bounded values and stable IDs.
- UI never invokes a transport, vault, SQL driver, Media3, AVFoundation, mpv,
  or DOM API.
- Production presenters map existing Air/protocol models; they do not map them
  into a second domain hierarchy.
- Design fixtures implement the same boundary and remain network-free.
- Navigation state is application state. Platform entry points only translate
  Back/escape/deep-link/lifecycle events.

### Dependency direction

```text
androidApp ─┐
desktopApp ─┼────> app:shared ─────> air ─────> iptv
webApp ─────┤             │          │ └──────> stremio-addon-client
iosApp ─────┘             │          └────────> video
                           │
                           ├─ androidMain ────> Android TV Material + video Media3 surface seam
                           ├─ jvmMain ─────────> mediamp-air desktop adapter
                           ├─ appleMain ───────> video AVFoundation surface seam
                           └─ wasmJsMain ──────> video browser surface seam

design app ───────────────> app:shared Android UI + design-only fixtures
```

`air`, protocol libraries, and `video` never depend upward on `app:shared`.
`app:shared/commonMain` never mentions platform engine types. The JVM runtime
may depend on `mediamp-air`; common code may depend only on `com.getair.video`.

### Source-set boundaries

`app:shared/commonMain` owns:

- bounded screen/presentation state and intents;
- navigation reducer and profile-aware route restoration;
- adapters from `HouseholdState`, `LocalSourceState`, catalog pages, EPG windows,
  continue-watching, and profile-library values into visible UI state;
- common Compose primitives that really compile and behave on all targets;
- player chrome driven only by `VideoPlayer`, `PlaybackState`, tracks,
  capabilities, and statistics;
- application lifetime interfaces and local-only composition policy.

`app:shared/androidMain` owns:

- the exact extracted TV Material screens and D-pad focus policy;
- phone/tablet Android layout selection without forking state models;
- Media3 surface attachment using the Android player returned by `video`;
- Android lifecycle, context-derived directories, and platform service wiring.

`app:shared/jvmMain` owns:

- desktop window-adaptive Compose root and keyboard/gamepad translation;
- `MediampDesktopBackendFactory` and `MediampDesktopVideoSurface` wiring;
- OS-specific data-directory selection and Linux/Windows/macOS vault selection.

OS checks in JVM code must be isolated to the composition factory, not spread
through screens. macOS cannot ship until a JVM Keychain-backed vault passes its
host tests.

`app:shared/appleMain` owns:

- `openAppleLocalApplicationState` and Native catalog construction;
- AVFoundation player/layer ownership and UIKit interop;
- lifecycle/background/audio-session/PiP integration without leaking Apple
  types into common state.

`app:shared/wasmJsMain` owns:

- browser local-state factory and IndexedDB catalog implementation;
- HTML video element attachment under the Compose player chrome;
- page visibility, browser history, keyboard, and storage-quota integration.

`commonTest` tests reducers, presenter paging, boot routing, profile/source
scope, and playback chrome against fakes. Platform UI and host integration stay
in their platform test source sets.

### Player surface boundary

The player chrome remains app-owned and backend-neutral. A platform source set
constructs one stable host containing a `VideoPlayer` and one composable surface
lambda/function. The common root can place that surface inline, in an in-app
PiP rectangle, or in a full-window layout without reopening media. The host is
closed by the application runtime, not by a recomposing screen.

Platform bindings are:

- Android: `AndroidMedia3BackendFactory`, attaching/detaching a `TextureView` or
  `SurfaceView` while retaining the player session.
- JVM desktop: `MediampDesktopBackendFactory` and
  `MediampDesktopVideoSurface`; no mediamp type escapes the JVM source set.
- iOS: `AppleAvFoundationBackendFactory` with the movable `AVPlayerLayer`
  hosted by UIKit interop.
- browser: `BrowserVideoBackendFactory` with one owned `HTMLVideoElement` moved
  with the in-app surface.

Do not create a universal engine-shaped UI abstraction. The common player API
already contains tracks, capabilities, errors, live kind, seekability, and
statistics. Plain live controls render no seek bar because
`PlaybackTimeline.showSeekBar` is false. A seekable live window may render its
real bounded range. Advanced statistics sample separately from browsing/control
state so frequent counters do not recompose the catalog.

### Local-only production boot sequence

The application starts without a server dependency or sign-in gate:

1. The platform entry point creates one supervised application scope and shows
   a constant, allocation-light boot surface.
2. On an I/O/native database dispatcher, open platform local state with
   `LocalApplicationSyncSources.None`, then open the platform durable catalog.
   `TvAuthController(null)` is valid local-only state; authentication is an
   optional future service, not a boot prerequisite.
3. Read the small household and source snapshots. If there is no profile, open
   local profile creation. If profiles exist but none is selected, open the
   profile chooser. Otherwise restore the selected profile and last valid route.
4. Render cached catalog pages and guide windows immediately. Never await a
   provider refresh before first interactive content.
5. Snapshot `sourcesFor(selectedProfile)` and schedule enabled sources through
   one `SourceRefreshCoordinator`. The source task reads its secret only at the
   transport boundary, dispatches by `LocalSourceKind`, streams bounded protocol
   batches into an unreachable catalog generation, and activates only after
   exact count validation.
6. Observe source revisions and visible page/window revisions, not the whole
   catalog. Profile changes cancel obsolete visible queries and refresh
   subscriptions without deleting another profile's/global source cache.
7. Lazy artwork requests use source-qualified stable keys, visible-size decode,
   cancellation, memory/disk bounds, and a small prefetch window. Artwork failure
   cannot block navigation or guide data.
8. On Play, resolve the selected source-qualified item to an ephemeral
   `PlaybackSource`. Acquire the source connection lease, use
   `LatestTuneCoordinator` for channel changes, and only then open the platform
   player. Provider URLs, headers, Stalker commands, addon credentials, and
   catch-up templates never enter UI state or durable catalog rows.
9. Record bounded progress/preferences through `LocalApplicationState`; plain
   live playback is not added to continue-watching. Release leases and stale
   native player completions deterministically.
10. On shutdown, cancel visible queries/refresh work, close the player, close
    the catalog, and cancel the owned application scope. Platform lifecycle
    suspension is not treated as process shutdown.

One `AirRuntime` composition object may own these long-lived values. Use
constructor injection and explicit close order. Do not add a DI container,
service locator, reflection, or classpath scanning.

### Platform storage and secret composition

| Shell | Local state | Catalog | Secret vault | Player/surface | Gate before shipping |
| --- | --- | --- | --- | --- | --- |
| Android TV/phone | `openAndroidLocalApplicationState` | `openAndroidDurableCatalogStore` | Android Keystore | Media3 | release APK on emulator and physical TV/phone |
| Linux JVM | `openLinuxLocalApplicationState` | `openJvmDurableCatalogStore` | KWallet | bundled mediamp/mpv | packaged host test and hardware decode |
| Windows JVM | `openWindowsLocalApplicationState` | `openJvmDurableCatalogStore` | DPAPI CurrentUser | bundled mediamp/mpv | signed/package-like Windows host test |
| macOS JVM | new JVM macOS factory | `openJvmDurableCatalogStore` | **missing JVM Keychain adapter** | bundled mediamp/mpv | Intel/Apple Silicon package and Keychain host tests |
| iOS | `openAppleLocalApplicationState` | `openNativeDurableCatalogStore` | Keychain | AVFoundation | simulator UI plus physical-device playback/PiP/power |
| Wasm browser | `openBrowserLocalApplicationState` | **in-flight, unverified direct IndexedDB store** | session memory only | HTML video | real-browser IndexedDB/quota/player matrix |

Use platform-private application directories. Suggested defaults are Android
private/no-backup storage, XDG state/data directories on Linux,
`%LOCALAPPDATA%/Air TV` on Windows, Application Support on macOS/iOS, and a
versioned browser namespace. Paths remain entry-point inputs to existing
factories rather than new global environment lookups in common code.

Never persist secrets in SQLDelight, IndexedDB, localStorage, Compose saveable
state, navigation arguments, crash reports, screenshots, or logs. Browser
provider credentials are intentionally forgotten at tab/session end until a
reviewed WebCrypto/server design exists. Do not weaken this to achieve parity.

### Catalog and UI performance invariants

The production shell may display any number of configured playlists and addons,
but catalog size cannot determine a frame's work:

- Query one bounded `DurableCatalogPage`/`DurableChannelPage` or EPG window at a
  time. Never convert the catalog or guide into a complete `StateFlow<List<...>>`.
- Use source-qualified stable keys everywhere; provider IDs are not globally
  unique.
- Keep guide now/next and visible-window updates separate from catalog rows.
- Compose only visible items plus a measured prefetch window. Do not eagerly
  decode artwork or construct playback URLs for list items.
- Map protocol rows to presentation values off the UI dispatcher. Publish one
  immutable visible snapshot per completed page/window, not one per parsed row.
- Keep player statistics, clock ticks, progress writes, guide time changes, and
  focus state in separate observable scopes so they cannot invalidate the home
  screen.
- Refresh work stays bounded and source-scoped. Failed refreshes keep the active
  generation; cancellation never clears usable data.
- Establish release-build cold-start, first-interactive, D-pad/touch response,
  p95 frame, visible-page query, artwork decode, playback-start, memory, and
  package-size budgets before expanding a shell.

## Smallest first executable shell

The first production executable is Android TV, not a multi-platform placeholder.
It has the strongest existing evidence: exact tested UI, a production local-state
factory, durable Android catalog, native Media3 surface, and canonical AVD.

Deliver it in four reviewable steps:

1. Create `app:shared` and `app:androidApp`; pin Kotlin 2.1.10, JDK 17, AGP
   8.13.2, Gradle 8.13, compile SDK 36, and a verified compatible Compose
   Multiplatform version.
2. Perform the one-time UI ownership migration above. The design APK consumes
   the extracted TV UI with identical fixture output and remains independently
   runnable.
3. Add the Android composition root. It opens real local household/source state
   and catalog, shows profile creation/selection or an empty/cached home without
   mocks, and creates one Media3 player host. No provider network call is needed
   to prove boot, persistence, route restoration, and player lifecycle.
4. Add one opt-in source refresh vertical slice after the cached/local shell is
   stable. It uses existing Xtream/M3U/Stremio contracts and atomic catalog
   activation; it never embeds integration credentials.

This slice is smaller and more truthful than creating empty desktop/iOS/web
modules, and it exercises the dependency direction every later shell will use.

## Packaging and GitHub-only publication

Library publication remains Maven-format GitHub Packages under `com.getair`.
Do not publish Air libraries to Maven Central merely to build the application.
Before the production repository switches from source composites to package
coordinates:

1. Add the same immutable release-triggered KMP GitHub Packages publication to
   `air` that IPTV/Stremio/video use, including Android, JVM, Apple, Native,
   JS, and Wasm variants built on their required hosts.
2. Run the authorized mediamp Air aggregation workflow once with immutable
   `video` and adapter versions, then compile a clean Kotlin 2.1 desktop
   consumer from GitHub Packages.
3. Pin all package versions in the application version catalog. Never use
   `latest`, branch artifacts, mutable snapshots, or an unqualified local Maven
   cache in release builds.

During the initial integration phase, Gradle composite substitution may use
GitHub checkouts of exact commits. Local sibling substitution must be explicit
(for example a `useLocalAirBuilds` property), not silently enabled in release
CI because a directory happens to exist.

Executable artifacts are not Maven libraries:

- Android Actions produce release APK/AAB artifacts; GitHub Releases may host
  internal/test builds. Play distribution, when requested, is a separate
  signed release job.
- Compose Desktop creates per-host DEB, MSI, and DMG distributions through
  `compose.desktop.application.nativeDistributions`. Bundle only the matching
  mediamp runtime architecture. Publish checksums/SBOMs with GitHub Releases.
- Xcode produces simulator/device archives. TestFlight/App Store delivery
  cannot be represented as a GitHub-only binary channel; signing and Apple
  delivery remain an explicitly authorized workflow.
- Web Actions publish immutable static Wasm assets to GitHub Pages/Releases
  after real-browser gates. Cache filenames are content-addressed and the
  service worker must not retain credential-bearing responses.

GitHub Actions receive package write/read tokens from `GITHUB_TOKEN` or scoped
repository secrets. Developer credentials stay in GitHub CLI/Gradle user
properties, never project files. Local workflow testing uses `air-act` and no
real integration credentials.

## Verification gates

### Extraction and common presentation

- Design APK assembles, lint passes, and its existing route/focus regression
  suite passes after the original source files are removed.
- A source scan proves production has no dependency on `com.getair.design`,
  `StaticData`, `mock.invalid`, or `air-player-test.mkv`.
- Presentation reducer tests cover cold local-only boot, no profile, selected
  profile, profile removal, any number of scoped sources, stale page results,
  failed refresh retention, process reopen, and route/focus restoration.
- Contract/ABI checks prove `shared/commonMain` exposes no Android, Media3,
  AVFoundation, mediamp, AWT, DOM, SQLDelight driver, or vault type.
- Large synthetic catalogs prove presenter memory and emitted item counts are
  bounded by requested visible pages, not total rows.

### Android TV

- Release APK on `air-tv-api36`, then a low-power physical TV device.
- Visible/deterministic focus, exact selected destination restoration, rapid
  repeated D-pad input, Back behavior, overscan-safe text, no touch dependency,
  and no navigation ANR/leak over a long soak.
- Four-card landscape rows and large guide/channel datasets maintain the agreed
  p95 frame/input budgets with lazy composition and bounded artwork.
- Cold/offline boot renders cached data, source/profile changes survive reopen,
  credentials remain in Keystore, and corrupted cache fails closed without
  deleting secrets.
- Live HLS/MPEG-TS, resilient buffer metrics, rapid tune latest-wins behavior,
  track selection, no seek bar for plain live, in-app movable surface, lifecycle
  detach/reattach, and advanced statistics use the corpus plus physical hardware.

### Android phone/tablet

- Same application ID/data store and shared presentation contracts as TV.
- Phone and tablet size classes, touch/keyboard, system Back, rotation,
  background/foreground, audio focus, in-app movable player, and Android system
  PiP where supported.
- Physical-device codec, power, thermal, memory, and startup measurements; an
  emulator compile is not release evidence.

### JVM desktop

- Unit/UI tests run on Linux, Windows, and macOS hosts; package smoke tests
  launch the installed distribution rather than only `jvmTest`.
- Linux KWallet, Windows DPAPI, and the future JVM macOS Keychain adapter each
  round-trip and delete credentials without prompts, plaintext files, or log
  exposure.
- One movable mediamp surface survives resize, in-app PiP movement, minimize,
  display scale changes, and reopen. Test hardware decode, subtitles/tracks,
  live recovery, shutdown, and native runtime architecture on real hosts.
- Validate DEB/MSI/DMG contents, startup time, memory, native library loading,
  and clean uninstall behavior.

### iOS

- `shared` framework compiles for simulator and device; Xcode lifecycle/UI tests
  cover boot, navigation, Keychain, database reopen, and UIKit surface movement.
- A physical device is mandatory for HLS/live buffering, MKV capability/failure,
  subtitles and tracks, HDR, audio session/interruption, backgrounding, system
  and in-app PiP, thermal/power, and memory evidence.
- No claim of tvOS support is made until all libraries declare and test tvOS
  targets separately.

### Browser

- Chrome, Firefox, Safari/WebKit, and supported Chromium derivatives run real
  Wasm UI tests and browser player capability tests; Node compilation is not
  browser evidence.
- IndexedDB generation activation, cancellation, upgrade, quota denial,
  eviction/rebuild, source isolation, paging, and guide-window tests pass before
  durable catalogs are advertised.
- A reload preserves non-secret state/catalog only. Provider secrets disappear
  at session end and never appear in localStorage, IndexedDB, service-worker
  caches, URLs owned by Air, logs, or crash output.
- Validate browser history/Back, visibility suspension, autoplay rejection,
  subtitle/track limits, HLS support differences, movable in-app surface, frame
  time, memory, and asset/cache size.

### CI and release

- Run focused common and Android tests before host matrices; cap local Gradle
  workers and never run multiple heavy native/emulator builds concurrently.
- Use `air-act` for workflow parsing/small Linux jobs. Architecture-specific
  Windows, macOS, iOS, and native-player gates run on their real hosted or
  physical targets.
- Release workflows verify dependency locks/version catalog, clean checkout,
  reproducible package coordinates, checksums, source/notice files, secret
  absence, and installed artifact smoke tests.
- A green common compile never labels another platform verified.

## Rejected alternatives

### Promote `design` directly to production

This preserves the current screen source but couples releases to mock state,
Android-only TV Material, a test asset, and direct Media3 construction. It also
makes desktop/Apple/web either depend on an Android application or fork it.

### Copy every screen into a new app

Two screen trees would drift immediately, and fixture verification would no
longer prove production behavior. The one-time move with design consuming the
canonical source provides the same visual reuse without this duplication.

### Force the Android TV screen into `commonMain`

The exact implementation uses Android TV Material and Android Back/surface
interop. Replacing those dependencies with home-grown wrappers merely to claim
source sharing increases boilerplate and changes tested focus semantics.
Share presentation state and truly portable primitives; keep platform input and
surface code at real platform edges.

### One giant executable KMP module

Android application, Compose Desktop packaging, browser executable, and Xcode
entry points have different plugins and release lifecycles. Combining them
makes Gradle configuration and host packaging conditional and fragile. One
shared module plus thin executable entries is the smallest conventional split.

### A module per feature or Clean Architecture layer

The protocol/core/player repositories already provide testable boundaries.
Feature/data/domain/use-case/DI modules would add interfaces and mapping without
independent publication, ownership, or runtime isolation.

### Require cloud account authentication before local boot

It contradicts local-first operation and turns an optional future sync service
into a startup dependency. Device-code/password authentication remains a
replaceable gateway and local-only state remains fully valid.

## Consequences and implementation blockers

The Android TV shell can begin after this plan is accepted. Cross-platform
completion remains deliberately blocked on real gaps rather than hidden
fallbacks:

- choose and compile-test a Compose Multiplatform version under Kotlin 2.1.10;
- remove direct `StaticData` use from canonical screens during the one-time move;
- implement production source-refresh adapters that stage the durable catalog;
- merge and verify direct IndexedDB catalog/EPG storage before web parity;
- implement a JVM macOS Keychain vault before macOS desktop credentials;
- authorize and prove immutable mediamp/video package publication;
- add `air` GitHub Packages publication or pin exact GitHub source composites;
- obtain physical Apple/Android and native desktop evidence before broad codec,
  zero-buffering, HDR, PiP, or power claims.

These gaps do not justify empty scaffolding, plaintext fallbacks, duplicated UI,
or platform types in common APIs.
