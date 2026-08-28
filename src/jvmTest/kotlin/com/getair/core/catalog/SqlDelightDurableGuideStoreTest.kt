package com.getair.core.catalog

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SqlDelightDurableGuideStoreTest {
    @Test
    fun sqliteBackendSatisfiesTheCommonGuideContract() = runTest {
        val directory = createTempDirectory("air-guide-contract-")
        val factory = JvmGuideContractFactory(directory)
        try {
            verifyDurableGuideStoreContract(factory)
        } finally {
            factory.close()
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun versionOneCatalogMigratesWithoutLosingLegacyRowsOrGuideTombstones() = runTest {
        val directory = createTempDirectory("air-guide-migration-")
        val databasePath = directory.resolve("catalog.db")
        try {
            Files.copy(
                Path.of("src/commonMain/sqldelight/databases/1.db"),
                databasePath,
                StandardCopyOption.REPLACE_EXISTING,
            )
            DriverManager.getConnection("jdbc:sqlite:${databasePath.toAbsolutePath()}").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("PRAGMA user_version=1")
                    statement.execute(
                        """
                        INSERT INTO catalog_source(
                          source_id, active_generation, next_generation, revision, activated_at_ms, deleted
                        ) VALUES ('legacy-source', 1, 2, 1, 1000, 0)
                        """.trimIndent(),
                    )
                    statement.execute(
                        "INSERT INTO catalog_generation(source_id, generation, staged_at_ms) VALUES ('legacy-source', 1, 1000)",
                    )
                    statement.execute(
                        """
                        INSERT INTO catalog_entry(
                          source_id, generation, kind, catalog_id, entity_id, sort_order,
                          display_name, payload_version, payload
                        ) VALUES ('legacy-source', 1, 'stremio', 'legacy', 'legacy-id', 0, 'Legacy', 1, '{}')
                        """.trimIndent(),
                    )
                }
            }

            var now = 10_000L
            var store = openStore(databasePath) { now }
            assertEquals(2L, schemaVersion(databasePath))
            assertEquals(1L, scalar(databasePath, "SELECT COUNT(*) FROM catalog_entry WHERE entity_id='legacy-id'"))
            val key = guideKey("migration", "main")
            val first = publishOne(store.guides, key)
            val deleted = store.guides.deleteGuide(key, first.revision, first.mutationEpoch)
                as DurableGuideDeleteResult.Deleted
            while (store.guides.cleanupUnreachable(1).hasMore) Unit
            store.close()

            now += 1_000
            store = openStore(databasePath) { now }
            val restored = publishOne(store.guides, key)
            assertTrue(restored.revision > deleted.revision)
            assertTrue(restored.generation > first.generation)
            store.close()
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun guideQueriesUseIndexesAndRawTablesContainNoProviderSecret() = runTest {
        val directory = createTempDirectory("air-guide-plan-")
        val path = directory.resolve("catalog.db")
        val secret = "never-persist-guide-secret"
        try {
            val store = openStore(path) { 42_000L }
            val guide = store.guides
            val key = guideKey("plans", "main")
            publishOne(guide, key)
            val plans = (guide as SqlDelightDurableGuideStore).queryPlansForTest()
            assertTrue(plans.any { "guide_channel" in it && "INDEX" in it }, plans.joinToString())
            assertTrue(
                plans.any { "guide_programme_timeline" in it || "guide_programme_overlap" in it },
                plans.joinToString(),
            )
            assertTrue(plans.any { "guide_programme_locator" in it }, plans.joinToString())
            assertTrue(plans.any { "guide_lease_expiry" in it }, plans.joinToString())
            assertTrue(plans.any { "guide_generation_expiry" in it }, plans.joinToString())
            assertTrue(plans.any { "guide_state" in it && "INDEX" in it }, plans.joinToString())
            assertTrue(plans.none { "USE TEMP B-TREE" in it }, plans.joinToString())

            val unsafe = runCatching {
                DurableGuideChannelRecord(
                    channelKey("unsafe"),
                    listOf("Unsafe"),
                    "https://images.example.test/art.jpg?token=$secret",
                )
            }
            assertTrue(unsafe.isFailure)
            store.close()

            DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                        """
                        SELECT source_key || feed_id || COALESCE(channel_key, '') || COALESCE(artwork_reference, '')
                        FROM (
                          SELECT source_key, feed_id, channel_key, artwork_reference FROM guide_channel
                          UNION ALL
                          SELECT source_key, feed_id, channel_key, artwork_reference FROM guide_programme
                        )
                        """.trimIndent(),
                    ).use { rows ->
                        while (rows.next()) assertFalse(secret in rows.getString(1))
                    }
                }
            }
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun sourceDeletionIsImmediateAndFanoutAdvancesOneFeedPerCleanupUnit() = runTest {
        val directory = createTempDirectory("air-guide-source-delete-")
        val path = directory.resolve("catalog.db")
        try {
            val store = openStore(path) { 50_000L }
            val sourceKey = guideKey("large-source", "seed").sourceKey
            repeat(64) { index ->
                publishOne(store.guides, DurableGuideKey(sourceKey, DurableGuideFeedId("feed-$index")))
            }
            val before = store.guides.sourceSnapshot(sourceKey)
            assertEquals(64, before.feedCount)
            val deleted = store.guides.deleteSource(sourceKey, before.token)
                as DurableGuideSourceDeleteResult.Deleted
            assertEquals(64, deleted.activeFeedCount)
            assertEquals(0, store.guides.sourceSnapshot(sourceKey).feedCount)
            assertTrue(store.guides.snapshots(store.guides.sourceSnapshot(sourceKey), limit = 10).snapshots.isEmpty())
            assertEquals(
                64,
                scalar(
                    path,
                    "SELECT COUNT(*) FROM guide_state WHERE source_key='${sourceKey.value}' AND active_generation IS NOT NULL",
                ),
            )

            val cleanup = store.guides.cleanupUnreachable(1)
            val work = (store.guides as SqlDelightDurableGuideStore).lastCleanupWorkForTest
            assertEquals(0, cleanup.removedRows)
            assertEquals(1, work.retiredSourceFeeds)
            assertEquals(1, work.total)
            assertEquals(
                63,
                scalar(
                    path,
                    "SELECT COUNT(*) FROM guide_state WHERE source_key='${sourceKey.value}' AND active_generation IS NOT NULL",
                ),
            )
            assertTrue(cleanup.hasMore)
            publishOne(store.guides, DurableGuideKey(sourceKey, DurableGuideFeedId("replacement")))
            val revived = store.guides.sourceSnapshot(sourceKey)
            assertEquals(1, revived.feedCount)
            assertEquals(
                listOf("replacement"),
                store.guides.snapshots(revived, limit = 10).snapshots.map { it.key.feedId.value },
            )
            repeat(4) { store.guides.cleanupUnreachable(1) }
            assertEquals(1, store.guides.sourceSnapshot(sourceKey).feedCount)
            store.close()
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun expiredWriterConsumesTheOnlyCleanupUnitBeforePayloadDeletion() = runTest {
        val directory = createTempDirectory("air-guide-expired-writer-")
        val path = directory.resolve("catalog.db")
        var now = 55_000L
        try {
            val store = openStore(path) { now }
            val key = guideKey("expired-writer", "main")
            val channel = channelKey("expired-writer")
            val generation = store.guides.beginRefresh(
                key,
                DurableGuideRetention(instant(1_000), instant(0), instant(10_000)),
            )
            store.guides.stage(
                generation,
                channels = listOf(DurableGuideChannelRecord(channel, listOf("Channel"))),
                programmes = listOf(
                    DurableGuideProgrammeRecord(channel, instant(1_000), instant(2_000), "Programme"),
                ),
            )
            now += store.guides.generationIdleTimeoutMillis + 1

            val first = store.guides.cleanupUnreachable(1)
            val firstWork = (store.guides as SqlDelightDurableGuideStore).lastCleanupWorkForTest
            assertEquals(0, first.removedRows)
            assertEquals(1, firstWork.expiredWriters)
            assertEquals(0, firstWork.removedPayloadRows)
            assertEquals(1, firstWork.total)
            assertTrue(first.hasMore)

            val second = store.guides.cleanupUnreachable(1)
            val secondWork = store.guides.lastCleanupWorkForTest
            assertEquals(1, second.removedRows)
            assertEquals(0, secondWork.expiredWriters)
            assertEquals(1, secondWork.removedPayloadRows)
            assertEquals(1, secondWork.total)
            store.close()
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun leaseAdmissionCapIsGlobalAcrossBackendOwners() = runTest {
        val directory = createTempDirectory("air-guide-global-leases-")
        val path = directory.resolve("catalog.db")
        var now = 60_000L
        try {
            val first = openStore(path) { now }
            val key = guideKey("leases", "main")
            publishOne(first.guides, key)
            val second = openStore(path) { now }
            val firstSnapshot = checkNotNull(first.guides.snapshot(key))
            val secondSnapshot = checkNotNull(second.guides.snapshot(key))
            val firstLeases = List(DurableGuideLimits.MAX_LIVE_LEASES / 2) {
                checkNotNull(first.guides.acquire(firstSnapshot))
            }
            val secondLeases = List(DurableGuideLimits.MAX_LIVE_LEASES - firstLeases.size - 1) {
                checkNotNull(second.guides.acquire(secondSnapshot))
            }
            val firstRace = async(Dispatchers.IO) {
                try {
                    Result.success(checkNotNull(first.guides.acquire(firstSnapshot)))
                } catch (failure: Throwable) {
                    Result.failure(failure)
                }
            }
            val secondRace = async(Dispatchers.IO) {
                try {
                    Result.success(checkNotNull(second.guides.acquire(secondSnapshot)))
                } catch (failure: Throwable) {
                    Result.failure(failure)
                }
            }
            val raced = listOf(firstRace.await(), secondRace.await())
            assertEquals(1, raced.count(Result<DurableGuideSnapshotLease>::isSuccess))
            assertEquals(1, raced.count { it.exceptionOrNull() is DurableGuideStoreException.Limit })
            val racedLease = raced.single { it.isSuccess }.getOrThrow()
            assertFailsWith<DurableGuideStoreException.Limit> { first.guides.acquire(firstSnapshot) }
            now += first.guides.leaseIdleTimeoutMillis + 1
            first.guides.cleanupUnreachable(1)
            val cleanupWork = (first.guides as SqlDelightDurableGuideStore).lastCleanupWorkForTest
            assertEquals(1, cleanupWork.expiredLeases)
            assertEquals(1, cleanupWork.total)
            assertEquals(
                (DurableGuideLimits.MAX_LIVE_LEASES - 1).toLong(),
                scalar(path, "SELECT COUNT(*) FROM guide_lease"),
            )
            val replacement = checkNotNull(first.guides.acquire(firstSnapshot))
            first.guides.release(replacement)
            if (raced.first().isSuccess) first.guides.release(racedLease) else second.guides.release(racedLease)
            firstLeases.forEach { first.guides.release(it) }
            secondLeases.forEach { second.guides.release(it) }
            second.close()
            first.close()
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun sourcePageAndTokenShareOneSqliteReadSnapshotAcrossConnections() = runTest {
        val directory = createTempDirectory("air-guide-source-race-")
        val path = directory.resolve("catalog.db")
        val validated = CountDownLatch(1)
        val continueRead = CountDownLatch(1)
        try {
            val reader = openStore(
                path,
                readProbe = GuideReadProbe {
                    validated.countDown()
                    check(continueRead.await(5, TimeUnit.SECONDS))
                },
                clock = { 70_000L },
            )
            val sourceKey = guideKey("race", "one").sourceKey
            publishOne(reader.guides, DurableGuideKey(sourceKey, DurableGuideFeedId("one")))
            val writer = openStore(path) { 70_000L }
            val source = reader.guides.sourceSnapshot(sourceKey)
            val page = async(Dispatchers.IO) { reader.guides.snapshots(source, limit = 10) }
            assertTrue(validated.await(5, TimeUnit.SECONDS))
            val writerStarted = CountDownLatch(1)
            val publish = async(Dispatchers.IO) {
                writerStarted.countDown()
                publishOne(writer.guides, DurableGuideKey(sourceKey, DurableGuideFeedId("two")))
            }
            assertTrue(writerStarted.await(5, TimeUnit.SECONDS))
            continueRead.countDown()
            val captured = page.await()
            publish.await()
            assertEquals(source.feedCount, captured.snapshots.size)
            assertFailsWith<DurableGuideStoreException.Stale> {
                reader.guides.snapshots(source, limit = 10)
            }
            writer.close()
            reader.close()
        } finally {
            continueRead.countDown()
            directory.toFile().deleteRecursively()
        }
    }

    private fun openStore(
        path: Path,
        readProbe: GuideReadProbe = GuideReadProbe.None,
        clock: () -> Long,
    ): SqlDelightDurableCatalogStore {
        val driver = JdbcSqliteDriver(
            url = "jdbc:sqlite:${path.toAbsolutePath()}",
            properties = Properties(),
            schema = AirCatalogDatabase.Schema,
        )
        driver.execute(null, "PRAGMA busy_timeout=5000", 0)
        return SqlDelightDurableCatalogStore(
            driver = driver,
            dispatcher = Dispatchers.IO.limitedParallelism(1),
            nowMillis = clock,
            guideReadProbe = readProbe,
        )
    }

    private suspend fun publishOne(
        store: DurableGuideStore,
        key: DurableGuideKey,
    ): DurableGuideSnapshot {
        val channel = channelKey("channel")
        val generation = store.beginRefresh(
            key,
            DurableGuideRetention(instant(1_000), instant(0), instant(10_000)),
        )
        store.stage(
            generation,
            channels = listOf(DurableGuideChannelRecord(channel, listOf("Channel"))),
            programmes = listOf(
                DurableGuideProgrammeRecord(
                    channelKey = channel,
                    start = instant(1_000),
                    end = instant(2_000),
                    title = "Programme",
                ),
            ),
        )
        return (store.activate(generation, DurableGuideCounts(1, 1)) as DurableGuideActivation.Published).snapshot
    }

    private fun guideKey(source: String, feed: String): DurableGuideKey =
        DurableGuideKey(DurableGuideSourceKey(digest(source)), DurableGuideFeedId(feed))

    private fun channelKey(value: String): DurableGuideChannelKey = DurableGuideChannelKey(digest(value))

    private fun digest(value: String): String = value.encodeToByteArray()
        .joinToString("") { byte -> (byte.toInt() and 0xff).toString(16).padStart(2, '0') }
        .padEnd(DurableGuideLimits.OPAQUE_DIGEST_CHARS, '0')
        .take(DurableGuideLimits.OPAQUE_DIGEST_CHARS)

    private fun instant(value: Long) = kotlinx.datetime.Instant.fromEpochMilliseconds(value)

    private fun schemaVersion(path: Path): Long = scalar(path, "PRAGMA user_version")

    private fun scalar(path: Path, sql: String): Long =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rows ->
                    check(rows.next())
                    rows.getLong(1)
                }
            }
        }
}

@OptIn(ExperimentalCoroutinesApi::class)
private class JvmGuideContractFactory(
    private val directory: Path,
) : DurableGuideStoreContractFactory {
    private data class OpenStore(
        val path: Path,
        val clock: ClockBox,
        val parent: SqlDelightDurableCatalogStore,
    )

    private class ClockBox(var millis: Long = 100_000L)

    private val stores = mutableMapOf<DurableGuideStore, OpenStore>()
    private var nextDatabase = 0

    override suspend fun create(): DurableGuideStore {
        val path = directory.resolve("guide-${nextDatabase++}.db")
        val clock = ClockBox()
        return open(path, clock)
    }

    override suspend fun reopen(store: DurableGuideStore): DurableGuideStore {
        val previous = checkNotNull(stores.remove(store))
        previous.parent.close()
        return open(previous.path, previous.clock)
    }

    override suspend fun advanceTimeBy(store: DurableGuideStore, milliseconds: Long) {
        check(milliseconds >= 0)
        checkNotNull(stores[store]).clock.millis += milliseconds
    }

    fun close() {
        stores.values.forEach { it.parent.close() }
        stores.clear()
    }

    private fun open(path: Path, clock: ClockBox): DurableGuideStore {
        val driver = JdbcSqliteDriver(
            url = "jdbc:sqlite:${path.toAbsolutePath()}",
            properties = Properties(),
            schema = AirCatalogDatabase.Schema,
        )
        val parent = SqlDelightDurableCatalogStore(
            driver = driver,
            dispatcher = Dispatchers.IO.limitedParallelism(1),
            nowMillis = { clock.millis },
        )
        return parent.guides.also { stores[it] = OpenStore(path, clock, parent) }
    }
}
