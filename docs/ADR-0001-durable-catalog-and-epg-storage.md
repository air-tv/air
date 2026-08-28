# ADR-0001: Durable source catalog and EPG storage

- Status: Accepted and implemented; performance and physical-device gates remain open
- Date: 2026-08-28
- Owners: Air application core

## Context

Air must retain independently refreshable catalogs and guide data for any number
of IPTV and Stremio sources without making catalog size determine UI work. The
current `LocalDocumentStore` remains the right storage for small household,
source-metadata, and continue-watching documents. Rewriting one serialized
document for a large catalog or XMLTV guide would make ingestion, time-window
queries, pruning, and recovery scale with the complete dataset, so it is not the
catalog store.

Catalog and guide records are rebuildable, non-secret caches. Xtream/Stalker
credentials, M3U/XMLTV URLs and headers, Stremio manifest URLs, bearer tokens,
and playback headers remain exclusively in `LocalSourceSecretStore`. Artwork
files also remain outside this database in a bounded image cache; catalog rows
store only the normalized contract's artwork references.

The decision must cover Android, JVM desktop, Apple and desktop Kotlin/Native,
and a realistic JS/Wasm browser implementation. Hot operations are source-
scoped catalog paging, now/next lookup, guide range lookup, search, refresh,
pruning, and source deletion.

## Decision

Use one small Air-owned catalog-store contract with two implementations:

1. Use SQLite through SQLDelight for Android, JVM desktop, and Kotlin/Native.
2. Use IndexedDB directly for JS and WasmJS. It implements the same logical
   operations, not SQLDelight or SQLite types.

The contract belongs beside the Air media repository; it does not become a new
module and it does not expose a driver, connection, DAO, generated query, SQL
row, IndexedDB handle, or platform type. Domain values at its boundary remain
the exact normalized models from `iptv` and `stremio-addon-client`.

No database dependency is added by this ADR. The first implementation change
must add the complete schema, driver factories, migrations, and the acceptance
tests below together; an empty database module or speculative generic DAO is
not allowed.

### Why SQLDelight

SQLDelight keeps the schema and query plan visible in `.sq` files, verifies SQL
at build time, and generates typed Kotlin call sites. Its documented SQLite
targets include Android, JVM, and Native on iOS, macOS, Linux, and Windows
([platform matrix](https://sqldelight.github.io/sqldelight/)). This gives Air
direct control of compound time indexes, bounded projections, source deletion,
and generation swaps without maintaining handwritten bind/column-mapping code.

Transactions roll back on exceptions, and SQLDelight supports explicit
transaction callbacks
([2.1.0 transaction API](https://sqldelight.github.io/sqldelight/2.1.0/multiplatform_sqlite/transactions/)).
Query invalidation can be exposed as `Flow` through the optional coroutine
extension
([2.1.0 coroutine API](https://sqldelight.github.io/sqldelight/2.1.0/multiplatform_sqlite/coroutines/)).
Migration files are ordered, can run transactionally when the driver supports
it, and can be verified against checked-in schema snapshots
([2.1.0 migration verification](https://sqldelight.github.io/sqldelight/2.1.0/multiplatform_sqlite/migrations/)).

The implementation pin under the current Kotlin 2.1.10 toolchain is SQLDelight
**2.1.0**. A local disposable spike generated and compiled one database with
the repository's Gradle 8.13 wrapper and Kotlin 2.1.10 for JVM and Linux x64.
SQLDelight 2.3.2 also compiled in the spike, but its published runtime requests
Kotlin stdlib 2.3.10
([2.3.2 runtime POM](https://repo.maven.apache.org/maven2/app/cash/sqldelight/runtime/2.3.2/runtime-2.3.2.pom)),
which would silently break this workspace's exact compiler/stdlib baseline.
Upgrade SQLDelight beyond 2.1.0 only with an explicit workspace Kotlin upgrade
and the full host matrix.

On JVM, `sqlite-driver:2.1.0` brings Xerial SQLite JDBC 3.49.1.0
([published POM](https://repo.maven.apache.org/maven2/app/cash/sqldelight/sqlite-driver/2.1.0/sqlite-driver-2.1.0.pom)).
The implementation must override that runtime to **3.51.3.0 or newer in the
same compatible line**, because SQLite documents a rare multi-connection WAL
corruption fix in 3.51.3
([SQLite WAL documentation](https://www.sqlite.org/wal.html#walreset)).
The override and effective dependency must be asserted in CI.

### Logical layout and atomic refresh

The SQLite and IndexedDB implementations mirror these logical records:

- active source generation and refresh metadata;
- channel, movie, series, and Stremio catalog entries keyed by source,
  generation, kind/catalog, and provider identity;
- EPG programmes keyed by source, generation, channel, start, and a stable
  provider/event discriminator;
- portable normalized search postings keyed by source, generation, token,
  entity kind, and entity identity.

Rows contain the small typed columns needed for filtering, ordering, and joins,
plus a versioned serialized payload of the exact normalized protocol model.
This avoids parallel application-shaped model classes while preventing list
screens from decoding payloads that are not visible. Payload decoding has
explicit byte and result limits.

A source refresh never edits its active generation in place:

1. Allocate an unreachable generation for that source.
2. Stream parser output into bounded batches. Commit each batch so a large
   XMLTV input does not create an unbounded transaction or WAL.
3. Check cancellation between decoded records and batches. A current native
   SQLite statement is not claimed to be cooperatively cancellable.
4. In one short transaction, validate the staged counts/invariants and move the
   source's active-generation pointer.
5. Publish one source revision, then prune old/unreachable generations in
   bounded chunks.

Failure, process death, or cancellation before step 4 leaves the previous
generation readable. Startup cleanup removes unreachable generations. Source
removal deletes only that source. Network refresh concurrency remains managed
by `SourceRefreshCoordinator`; database writes use one bounded writer queue so
many sources cannot create lock contention or an unbounded write backlog.

On Native, all statements in one transaction stay on the same database
dispatcher because the native driver aligns transactions with a connection and
thread
([native-driver concurrency notes](https://sqldelight.github.io/sqldelight/2.1.0/2.x/drivers/native-driver/app.cash.sqldelight.driver.native/-native-sqlite-driver/)).
Public Air methods remain `suspend`; blocking JVM/native work never executes on
the UI dispatcher.

### Guide and search indexes

The baseline schema uses ordinary, portable indexes rather than requiring
FTS5:

- `(source_id, generation, channel_id, start_ms)` for channel-range scans;
- a companion index beginning with `(source_id, generation, channel_id,
  end_ms)` for now/next and expiry scans;
- source/generation/order indexes for bounded catalog pages;
- source/generation/token/entity keys for incremental search postings.

Exact column order must be proven with `EXPLAIN QUERY PLAN` against the final
queries. SQLite supports multi-column and covering indexes
([query planner](https://www.sqlite.org/queryplanner.html)); partial indexes may
reduce file and write cost when a predicate matches the real workload
([partial indexes](https://www.sqlite.org/partialindex.html)). Do not add an
index without a measured query that uses it.

FTS5 remains an optional internal optimization after capability and size tests,
not part of the store contract. Platform SQLite versions and browser IndexedDB
do not provide one uniform FTS5 guarantee. FTS5 prefix/trigram indexes can
accelerate broader matching but trade extra index size and write work
([SQLite FTS5](https://www.sqlite.org/fts5.html)); Air's portable postings keep
search semantics and fixtures identical on every backend.

Guide queries always require a bounded time window and page/row limit. Retention
is source configurable within global disk bounds. No API emits the entire guide
or complete catalog as a `StateFlow`; observable state contains revisions and
small visible windows only.

### Browser implementation

IndexedDB supplies atomic transactions, ordered indexes, key ranges, and cursors
without shipping a second SQLite WebAssembly runtime. Its specification notes
that read/write transactions are atomic and that overlapping writers are
serialized
([IndexedDB 3.0](https://www.w3.org/TR/IndexedDB/#transaction-construct)).
Browser transactions stay short: stage bounded batches, then switch the active
generation in one read/write transaction. Coroutine cancellation aborts the
current transaction when possible and otherwise leaves an unreachable staging
generation for cleanup.

Use compound indexes equivalent to the SQLite keys above. IndexedDB has no FTS
query language, so it persists the same normalized postings. JS and WasmJS share
the Air contract and fixtures but have thin target-specific promise/DOM bridges.

Browser catalog data is a rebuildable cache. Storage is best-effort by default,
quota and eviction policy vary by browser, and `navigator.storage.persist()` is
a request rather than a guarantee
([browser storage policy](https://developer.mozilla.org/en-US/docs/Web/API/Storage_API/Storage_quotas_and_eviction_criteria)).
Air may request persistence after an explicit source is added, must handle
denial/quota errors, and must recover by refreshing. Secrets never enter
IndexedDB.

## Rejected alternatives

### Room 3.0.2

Room 3.0.2 is a real KMP option, not an Android-only dismissal. Room 3 adds
JS/WasmJS, coroutine-first operations, Flow invalidation, and FTS5
([Room 3 release notes](https://developer.android.com/jetpack/androidx/releases/room3)).
Its KMP setup can use bundled SQLite consistently across platforms
([Room KMP driver setup](https://developer.android.com/kotlin/multiplatform/room#select-sqlite-driver)).

It loses here because Air would maintain entities, DAO annotations, query
strings, KSP configuration for every target, generated schema JSON, and domain
mapping for protocol-owned models. Room 3 requires KSP and Kotlin-only codegen,
and its own guidance recommends concentrating Room in a separate module
([Room 3 Kotlin/codegen notes](https://developer.android.com/jetpack/androidx/releases/room3#kotlin-and-coroutines-first)).
JVM/Android builds also retain generated database constructors for reflective
lookup, requiring an obfuscation keep rule
([Room KMP minification note](https://developer.android.com/kotlin/multiplatform/room#minification-and-obfuscation)).
That conflicts with Air's no-reflection rule and adds more structure than a
small explicit SQL store.

### Raw AndroidX SQLite or direct C/JDBC wrappers

AndroidX's `BundledSQLiteDriver` gives a current, consistent SQLite build, but
its low-level API is connections, prepared statements, manual binding, stepping,
and column extraction
([AndroidX SQLite KMP migration guide](https://developer.android.com/kotlin/multiplatform/sqlite)).
Air would own query code generation, mapping, invalidation, migration
verification, pooling, and cancellation glue. Direct sqlite3/JDBC wrappers have
the same maintenance cost plus more native packaging. Keep this as a fallback
only if the SQLDelight implementation fails a measured runtime or package gate.

### SQLDelight web-worker/sql.js driver

SQLDelight's current browser path is asynchronous and runs queries in a Web
Worker, but it requires the worker package, sql.js, and a copied SQLite Wasm
binary
([2.3.2 SQL.js worker setup](https://sqldelight.github.io/sqldelight/2.3.2/js_sqlite/sqljs_worker/)).
It is browser-only and still needs a separately designed durable backing path.
IndexedDB is already the browser's indexed durable store, works for both Air JS
and WasmJS wrappers, and avoids the extra engine and worker assets.

### Serialized catalog/guide documents

Whole-source JSON documents make replacement simple but force full-file parse,
allocation, and rewrite for range queries and incremental refresh. They also
cannot provide bounded indexed now/next and search reads. `LocalDocumentStore`
therefore remains limited to small state.

## Implementation and acceptance gates

Before the dependency lands, the implementation PR must prove all of the
following with release-like settings and `--max-workers=2` locally:

- checked-in initial schema plus migration snapshot, `verifyMigrations=true`,
  and upgrade tests from every released schema;
- atomic generation activation under parser failure, cancellation, quota/disk
  failure, and simulated reopen after an incomplete stage;
- bounded-memory ingestion of the largest fixture at the configured limits;
- source isolation and deletion with many enabled sources;
- indexed now/next, guide-window, catalog-page, and search queries, including
  asserted query plans on SQLite and compound-index cursor tests on IndexedDB;
- no whole-catalog/whole-guide decode on open or UI observation;
- cold open, batch write throughput, p50/p95 visible-window query latency,
  peak memory, database size, and old-generation prune time;
- per-target packaged-size delta. The JVM measurement must account for the
  multi-platform Xerial native jar rather than quoting only SQLDelight's Kotlin
  runtime size;
- Android emulator plus low-power physical-TV measurements, native host tests,
  Windows/macOS CI, and real JS/Wasm browser tests;
- an effective-dependency assertion for SQLDelight, Kotlin stdlib, and SQLite,
  with no credentials, provider URLs, response bodies, or artwork bytes in the
  database or diagnostics.

WAL is enabled only for a runtime SQLite known to include the 3.51.3 fix or a
documented vendor backport, and only after the connection/checkpoint test passes.
Otherwise the store uses serialized writes with the rollback journal. This is
an internal driver decision and never changes the common contract.

### Browser implementation checkpoint (2026-08-28)

JS and WasmJS now implement the common store directly on IndexedDB without a
SQLite Wasm runtime. Source state, generation records, catalog identities,
ordered catalog indexes, channel identities, ordered channel indexes, EPG
programmes, bounded counters, and an orphan queue are separate object-store
concerns. Catalog and channel ordering use native secondary indexes; catalog,
guide, and cleanup reads use bounded cursors.

Refresh allocation, batch staging, exact-count activation, deletion, and prune
steps each run in short native read/write transactions. A failed, cancelled,
quota-exhausted, or abandoned stage cannot move the active-generation pointer.
The coroutine bridge requests `IDBTransaction.abort()` on cancellation; a
transaction already committed before the cancellation signal remains committed
and is either active or an unreachable cleanup candidate.

The same deterministic contract fixture passed Kotlin/JS and Kotlin/WasmJS in
Chromium 151 through Karma's real `ChromeHeadless` launcher. That proves the
two compiled browser artifacts execute native IndexedDB transactions and
cursors; it is not Firefox or Safari evidence. Safari private browsing, quota
policy, eviction, and multi-tab version-upgrade behavior remain explicit
browser-matrix risks.
