package com.getair.core.catalog

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.ChannelId
import com.getair.iptv.model.ChannelKind
import com.getair.iptv.model.EpisodeId
import com.getair.iptv.model.IptvChannelMetadata
import com.getair.iptv.model.IptvEpisodeMetadata
import com.getair.iptv.model.IptvMovieMetadata
import com.getair.iptv.model.IptvSeriesMetadata
import com.getair.iptv.model.IptvSourceKind
import com.getair.iptv.model.MovieId
import com.getair.iptv.model.SeriesId
import com.getair.stremio.model.MetaLink
import com.getair.stremio.model.MetaPreview
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Properties
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Instant
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SqlDelightDurableCatalogStoreTest {
    @Test
    fun activatesExactCountsAndPagesOnlyTheActiveGeneration() = withDatabase { path ->
        val store = openStore(path)
        val source = source("one")
        val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "popular")

        val first = store.beginRefresh(source)
        store.stageCatalogBatch(first, catalog, metas("old", 3))
        val firstStatus = store.activate(first, CatalogGenerationCounts(3, 0, 0))
        assertEquals(1, firstStatus.revision)
        assertEquals(first.value, firstStatus.activeGeneration)

        val second = store.beginRefresh(source)
        store.stageCatalogBatch(second, catalog, metas("new", 5))
        assertFailsWith<CatalogStoreException> {
            store.activate(second, CatalogGenerationCounts(4, 0, 0))
        }
        assertEquals(
            listOf("old-0", "old-1", "old-2"),
            store.catalogPage(source, catalog, limit = 10).items.map { itemId(it.item) },
        )

        store.activate(second, CatalogGenerationCounts(5, 0, 0))
        val pageOne = store.catalogPage(source, catalog, limit = 2)
        val pageTwo = store.catalogPage(source, catalog, pageOne.nextCursor, limit = 2)
        val pageThree = store.catalogPage(source, catalog, pageTwo.nextCursor, limit = 2)
        assertEquals(
            listOf("new-0", "new-1", "new-2", "new-3", "new-4"),
            (pageOne.items + pageTwo.items + pageThree.items).map { itemId(it.item) },
        )
        assertNull(pageThree.nextCursor)
        assertTrue(first.toString().contains("<redacted>"))
        assertFalse(first.toString().contains(source.value))
        store.close()
    }

    @Test
    fun transactionFailureCancellationAndReopenPreserveThePreviousGeneration() =
        withDatabase { path ->
            var injected: Throwable? = null
            val probe = CatalogWriteProbe { index ->
                if (index == 1) injected?.let { throw it }
            }
            var store = openStore(path, probe)
            val source = source("atomic")
            val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "safe")
            val active = store.beginRefresh(source)
            store.stageCatalogBatch(active, catalog, metas("active", 2))
            store.activate(active, CatalogGenerationCounts(2, 0, 0))

            val failed = store.beginRefresh(source)
            injected = IllegalStateException("simulated storage failure")
            assertFailsWith<CatalogStoreException> {
                store.stageCatalogBatch(failed, catalog, metas("failed", 3))
            }
            assertFailsWith<CatalogStoreException> {
                store.activate(failed, CatalogGenerationCounts(3, 0, 0))
            }

            val cancelled = store.beginRefresh(source)
            injected = CancellationException("cancelled")
            assertFailsWith<CancellationException> {
                store.stageCatalogBatch(cancelled, catalog, metas("cancelled", 3))
            }
            injected = null
            assertEquals(
                listOf("active-0", "active-1"),
                store.catalogPage(source, catalog, limit = 10).items.map { itemId(it.item) },
            )
            store.close()

            store = openStore(path)
            assertEquals(
                listOf("active-0", "active-1"),
                store.catalogPage(source, catalog, limit = 10).items.map { itemId(it.item) },
            )
            store.close()
        }

    @Test
    fun guideQueriesAreBoundedIndexedAndSourceScoped() = withDatabase { path ->
        val store = openStore(path)
        val source = source("guide-a")
        val other = source("guide-b")
        val channelOne = EpgChannel(EpgChannelId("channel-1"), listOf("One"))
        val channelTwo = EpgChannel(EpgChannelId("channel-2"), listOf("Two"))

        val generation = store.beginRefresh(source)
        store.stageGuideBatch(
            generation,
            channels = listOf(channelOne, channelTwo),
            programmes = listOf(
                programme("one-a", "channel-1", 1_000, 2_000),
                programme("one-b", "channel-1", 2_000, 3_000),
                programme("two-a", "channel-2", 1_000, 3_000),
            ),
        )
        store.activate(generation, CatalogGenerationCounts(0, 2, 3))

        val otherGeneration = store.beginRefresh(other)
        store.stageGuideBatch(
            otherGeneration,
            channels = listOf(EpgChannel(EpgChannelId("channel-1"), listOf("Other"))),
            programmes = listOf(programme("other", "channel-1", 1_000, 4_000)),
        )
        store.activate(otherGeneration, CatalogGenerationCounts(0, 1, 1))

        val firstChannel = store.channelPage(source, limit = 1)
        assertEquals("One", firstChannel.items.single().channel.displayNames.single())
        assertEquals(
            "Two",
            store.channelPage(source, firstChannel.nextCursor, limit = 1)
                .items.single().channel.displayNames.single(),
        )

        val window = store.guideWindow(
            source,
            EpgChannelId("channel-1"),
            instant(1_500),
            instant(2_500),
            limit = 5,
        )
        assertEquals(listOf("one-a", "one-b"), window.map(DurableGuideProgramme::providerEventId))
        val nowNext = store.nowNext(source, EpgChannelId("channel-1"), instant(1_500))
        assertEquals("one-a", nowNext.current?.title)
        assertEquals("one-b", nowNext.next?.title)
        assertEquals(
            "other",
            store.nowNext(other, EpgChannelId("channel-1"), instant(1_500)).current?.title,
        )

        val (catalogPlan, guidePlan) = store.queryPlansForTest()
        assertTrue(catalogPlan.any { "catalog_entry_order" in it }, catalogPlan.joinToString())
        assertTrue(
            guidePlan.any {
                "SEARCH p USING INDEX" in it &&
                    "source_id=? AND generation=? AND channel_id=? AND start_ms<?" in it
            },
            guidePlan.joinToString(),
        )
        assertTrue(guidePlan.none { "USE TEMP B-TREE" in it }, guidePlan.joinToString())
        store.close()
    }

    @Test
    fun oneWriterSerializesConcurrentBoundedBatches() = withDatabase { path ->
        val store = openStore(path)
        val source = source("concurrent")
        val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "all")
        val generation = store.beginRefresh(source)

        coroutineScope {
            (0 until 8).map { batch ->
                async(Dispatchers.Default) {
                    store.stageCatalogBatch(generation, catalog, metas("batch-$batch", 32))
                }
            }.awaitAll()
        }
        store.activate(generation, CatalogGenerationCounts(256, 0, 0))

        val ids = mutableSetOf<String>()
        var cursor: Long? = null
        do {
            val page = store.catalogPage(source, catalog, cursor, 37)
            ids += page.items.map { itemId(it.item) }
            cursor = page.nextCursor
        } while (cursor != null)
        assertEquals(256, ids.size)
        assertFailsWith<IllegalArgumentException> {
            store.stageCatalogBatch(
                store.beginRefresh(source),
                catalog,
                metas("too-many", MAX_BATCH_ITEMS + 1),
            )
        }
        store.close()
    }

    @Test
    fun credentialShapedReferencesInlineArtworkAndPlaybackPayloadsCannotEnterTheStore() =
        withDatabase { path ->
            val store = openStore(path)
            val source = source("secrets")
            val generation = store.beginRefresh(source)
            val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "safe")
            val secret = "never-store-this-password"

            val credentialFailure = assertFailsWith<IllegalArgumentException> {
                store.stageCatalogBatch(
                    generation,
                    catalog,
                    listOf(
                        DurableCatalogItem.Stremio(
                            MetaPreview(
                                id = "credential",
                                type = "movie",
                                name = "Credential",
                                poster = "https://user:$secret@example.test/poster.jpg",
                            ),
                        ),
                    ),
                )
            }
            assertFalse(credentialFailure.message.orEmpty().contains(secret))
            assertFailsWith<IllegalArgumentException> {
                store.stageCatalogBatch(
                    generation,
                    catalog,
                    listOf(
                        DurableCatalogItem.Stremio(
                            MetaPreview(
                                id = "inline",
                                type = "movie",
                                name = "Inline",
                                poster = "data:image/png;base64,AAAA",
                            ),
                        ),
                    ),
                )
            }
            val metadataFailure = assertFailsWith<IllegalArgumentException> {
                store.stageCatalogBatch(
                    generation,
                    DurableCatalogKey(DurableCatalogKind.IptvMovie, "movies"),
                    listOf(
                        DurableCatalogItem.IptvMovie(
                            IptvMovieMetadata(
                                id = MovieId("movie-secret"),
                                name = "Movie",
                                containerExtension = "mkv",
                                posterUrl = "https://images.example.test/poster.jpg?token=$secret",
                            ),
                        ),
                    ),
                )
            }
            assertFalse(metadataFailure.message.orEmpty().contains(secret))
            // Only metadata variants exist. Their protocol contracts have no stream
            // URL, direct source, or request-header field.
            assertEquals(
                setOf("Stremio", "IptvChannel", "IptvMovie", "IptvSeries", "IptvEpisode"),
                DurableCatalogItem::class.java.declaredClasses.map { it.simpleName }.toSet(),
            )
            store.close()

            DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT payload FROM catalog_entry").use { rows ->
                        while (rows.next()) assertFalse(rows.getString(1).contains(secret))
                    }
                }
            }
        }

    @Test
    fun deletionIsImmediateAndCleanupIsChunkBounded() = withDatabase { path ->
        var store: DurableCatalogStore = openStore(path)
        val source = source("delete")
        val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "large")
        val active = store.beginRefresh(source)
        store.stageCatalogBatch(active, catalog, metas("first", 256))
        store.stageCatalogBatch(active, catalog, metas("second", 256))
        store.activate(active, CatalogGenerationCounts(512, 0, 0))

        store.deleteSource(source)
        assertNull(store.sourceStatus(source))
        assertTrue(store.catalogPage(source, catalog, limit = 10).items.isEmpty())
        store.close()

        // Opening performs one bounded startup cleanup chunk, never a full scan.
        store = openJvmDurableCatalogStore(
            path,
            CatalogStorageOptions(startupCleanupRows = 100),
        )
        val next = store.cleanupUnreachable(100)
        assertTrue(next.removedRows <= 100)
        assertTrue(next.hasMore)
        var passes = 0
        var result = next
        while (result.hasMore) {
            result = store.cleanupUnreachable(100)
            passes++
            assertTrue(result.removedRows <= 100)
        }
        assertTrue(passes >= 3)
        store.close()
    }

    @Test
    fun aDeletedOrSupersededRefreshCannotReviveStaleData() = withDatabase { path ->
        val store = openStore(path)
        val source = source("stale")
        val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "all")
        val stale = store.beginRefresh(source)
        store.stageCatalogBatch(stale, catalog, metas("stale", 1))
        store.deleteSource(source)
        assertFailsWith<CatalogStoreException> {
            store.activate(stale, CatalogGenerationCounts(1, 0, 0))
        }

        val superseded = store.beginRefresh(source)
        val latest = store.beginRefresh(source)
        assertFailsWith<CatalogStoreException> {
            store.stageCatalogBatch(superseded, catalog, metas("superseded", 1))
        }
        store.stageCatalogBatch(latest, catalog, metas("latest", 1))
        store.activate(latest, CatalogGenerationCounts(1, 0, 0))
        assertEquals(
            listOf("latest-0"),
            store.catalogPage(source, catalog, limit = 10).items.map { itemId(it.item) },
        )
        store.close()
    }

    @Test
    fun safeIptvSeriesPayloadRoundTripsWithoutPlaybackFields() = withDatabase { path ->
        val store = openStore(path)
        val source = source("series")
        val catalog = DurableCatalogKey(DurableCatalogKind.IptvSeries, "drama")
        val generation = store.beginRefresh(source)
        val series = IptvSeriesMetadata(
            id = SeriesId("series-1"),
            name = "Series",
            coverUrl = "https://images.example.test/series.jpg",
            plot = "Plot",
        )
        store.stageCatalogBatch(
            generation,
            catalog,
            listOf(DurableCatalogItem.IptvSeries(series)),
        )
        store.activate(generation, CatalogGenerationCounts(1, 0, 0))
        val item = store.catalogPage(source, catalog, limit = 1).items.single().item
        assertEquals(series, assertIs<DurableCatalogItem.IptvSeries>(item).value)

        val channelCatalog = DurableCatalogKey(DurableCatalogKind.IptvChannel, "live")
        val movieCatalog = DurableCatalogKey(DurableCatalogKind.IptvMovie, "movies")
        val episodeCatalog = DurableCatalogKey(DurableCatalogKind.IptvEpisode, "series-1")
        val additional = store.beginRefresh(source("metadata-all"))
        store.stageCatalogBatch(
            additional,
            channelCatalog,
            listOf(
                DurableCatalogItem.IptvChannel(
                    IptvChannelMetadata(
                        id = ChannelId("channel-1"),
                        name = "Channel",
                        source = IptvSourceKind.Xtream,
                        kind = ChannelKind.Live,
                        logoUrl = "https://images.example.test/channel.png",
                    ),
                ),
            ),
        )
        store.stageCatalogBatch(
            additional,
            movieCatalog,
            listOf(
                DurableCatalogItem.IptvMovie(
                    IptvMovieMetadata(
                        id = MovieId("movie-1"),
                        name = "Movie",
                        containerExtension = "mkv",
                        posterUrl = "https://images.example.test/movie.jpg",
                    ),
                ),
            ),
        )
        store.stageCatalogBatch(
            additional,
            episodeCatalog,
            listOf(
                DurableCatalogItem.IptvEpisode(
                    IptvEpisodeMetadata(
                        id = EpisodeId("episode-1"),
                        seriesId = SeriesId("series-1"),
                        title = "Episode",
                        season = 1.0,
                        episode = 1.0,
                        containerExtension = "mkv",
                    ),
                ),
            ),
        )
        store.activate(additional, CatalogGenerationCounts(3, 0, 0))
        assertIs<DurableCatalogItem.IptvChannel>(
            store.catalogPage(source("metadata-all"), channelCatalog, limit = 1).items.single().item,
        )
        assertIs<DurableCatalogItem.IptvMovie>(
            store.catalogPage(source("metadata-all"), movieCatalog, limit = 1).items.single().item,
        )
        assertIs<DurableCatalogItem.IptvEpisode>(
            store.catalogPage(source("metadata-all"), episodeCatalog, limit = 1).items.single().item,
        )
        store.close()
    }

    @Test
    fun publicStoreApiDoesNotExposeSqlDelightOrGeneratedDatabaseTypes() {
        val publicTypes = listOf(
            DurableCatalogStore::class.java,
            DurableCatalogPage::class.java,
            DurableChannelPage::class.java,
            CatalogGeneration::class.java,
        )
        val signatures = publicTypes.flatMap { type ->
            type.methods.map { it.toGenericString() } +
                type.constructors.map { it.toGenericString() }
        }
        assertTrue(signatures.none { "app.cash.sqldelight" in it || ".catalog.db." in it })
    }

    private fun openStore(
        path: Path,
        probe: CatalogWriteProbe = CatalogWriteProbe.None,
    ): SqlDelightDurableCatalogStore {
        val driver = JdbcSqliteDriver(
            url = "jdbc:sqlite:${path.toAbsolutePath()}",
            properties = Properties(),
            schema = AirCatalogDatabase.Schema,
        )
        return SqlDelightDurableCatalogStore(
            driver = driver,
            dispatcher = Dispatchers.IO.limitedParallelism(1),
            nowMillis = { 42_000 },
            writeProbe = probe,
        )
    }

    private fun <T> withDatabase(block: suspend (Path) -> T) = runTest {
        val directory = createTempDirectory("air-catalog-test-")
        try {
            block(directory.resolve("catalog.db"))
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    private fun source(id: String) = LocalSourceId("source-$id")

    private fun metas(prefix: String, count: Int): List<DurableCatalogItem> =
        List(count) { index ->
            DurableCatalogItem.Stremio(
                MetaPreview(
                    id = "$prefix-$index",
                    type = "movie",
                    name = "$prefix $index",
                    poster = "https://images.example.test/$prefix/$index.jpg",
                    links = listOf(MetaLink("genre", "genre", "stremio://genre/$index")),
                ),
            )
        }

    private fun itemId(item: DurableCatalogItem): String = when (item) {
        is DurableCatalogItem.Stremio -> item.value.id
        is DurableCatalogItem.IptvChannel -> item.value.id.value
        is DurableCatalogItem.IptvMovie -> item.value.id.value
        is DurableCatalogItem.IptvSeries -> item.value.id.value
        is DurableCatalogItem.IptvEpisode -> item.value.id.value
    }

    private fun programme(
        eventId: String,
        channelId: String,
        startMs: Long,
        endMs: Long,
    ) = DurableGuideProgramme(
        eventId,
        EpgProgramme(
            channelId = EpgChannelId(channelId),
            start = instant(startMs),
            end = instant(endMs),
            title = eventId,
        ),
    )

    private fun instant(milliseconds: Long): Instant = Instant.fromEpochMilliseconds(milliseconds)
}
