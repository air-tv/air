package com.getair.core.catalog

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.DriverManager
import java.util.Properties
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
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

    private fun openStore(path: Path, clock: () -> Long): SqlDelightDurableCatalogStore {
        val driver = JdbcSqliteDriver(
            url = "jdbc:sqlite:${path.toAbsolutePath()}",
            properties = Properties(),
            schema = AirCatalogDatabase.Schema,
        )
        return SqlDelightDurableCatalogStore(
            driver = driver,
            dispatcher = Dispatchers.IO.limitedParallelism(1),
            nowMillis = clock,
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
