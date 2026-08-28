package com.getair.core.catalog

import com.getair.core.source.LocalSourceId
import com.getair.iptv.EpgFeedId
import com.getair.iptv.EpgGuideKey
import com.getair.iptv.EpgRetentionPolicy
import com.getair.iptv.EpgStoreException
import com.getair.iptv.model.EpgBatch
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgMatchResult
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.IptvPlaylistEntry
import com.getair.iptv.model.PlaylistEntryId
import com.getair.iptv.model.PlaylistEntryKind
import com.getair.iptv.model.SourceId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DurableCatalogEpgStoreTest {
    @Test
    fun refreshProjectionQueriesMatchingIsolationAndReopen() = runTest {
        val backend = ContractInMemoryDurableGuideStore()
        val catalog = GuideOnlyCatalogStore(backend)
        val adapter = DurableCatalogEpgStore(
            catalog,
            retention = EpgRetentionPolicy(keepPastSeconds = 10, keepFutureSeconds = 100),
        )
        val secret = "provider-password"
        val rawChannel = EpgChannelId("https://provider.example.test/live?password=$secret")
        val guide = guide("source-one", "feed-one")
        val anchor = instant(10_000)
        val channel = EpgChannel(
            rawChannel,
            listOf("News One"),
            iconUrl = "https://images.example.test/icon.png?token=$secret",
            urls = listOf("https://provider.example.test/$secret"),
        )
        val first = programme(rawChannel, 9_000, 12_000, "Morning")
        val duplicate = first.copy(title = "Z Morning")
        val open = programme(rawChannel, 13_000, null, "Open")
        val invalid = programme(rawChannel, 15_000, 14_000, "Invalid")
        val outside = programme(rawChannel, 1_000_000, 1_001_000, "Outside")
        val refresh = adapter.refresh(
            guide,
            flowOf(EpgBatch(listOf(channel), listOf(first, duplicate, open, invalid, outside), 1, 5)),
            anchor,
        )
        assertTrue(refresh.committed)
        assertEquals(1, refresh.channelCount)
        assertEquals(2, refresh.programmeCount)
        assertEquals(1, refresh.deduplicatedProgrammeRows)
        assertEquals(1, refresh.discardedInvalid)
        assertEquals(1, refresh.discardedOutsideRetention)

        val info = assertNotNull(adapter.snapshotInfo(guide))
        assertEquals(refresh.revision, info.revision)
        val window = adapter.visibleWindow(guide, listOf(rawChannel), instant(8_000), instant(20_000))
        assertEquals(1, window.channels.size)
        assertEquals(rawChannel, window.channels.single().channelId)
        assertNull(window.channels.single().channel?.iconUrl)
        assertTrue(window.channels.single().channel?.urls.orEmpty().isEmpty())
        assertEquals(listOf("Morning", "Open"), window.channels.single().programmes.map(EpgProgramme::title))
        assertTrue(window.channels.single().programmes.all { it.channelId == rawChannel })

        val nowNext = adapter.nowNext(guide, rawChannel, instant(11_000))
        assertEquals("Morning", nowNext.value.current?.title)
        assertEquals("Open", nowNext.value.next?.title)

        val projection = assertNotNull(adapter.programmeSearchProjection(guide, refresh.revision))
        val rows = mutableListOf<com.getair.iptv.EpgProgrammeSearchRow>()
        projection.rows.collect { rows += it }
        assertEquals(2, rows.size)
        val resolved = assertNotNull(adapter.programme(guide, rows.first().locator, info.revision))
        assertFalse(resolved.channelId.value.contains(secret))
        assertNull(adapter.programme(guide, rows.first().locator, info.revision + 1))

        val entry = IptvPlaylistEntry(
            PlaylistEntryId("entry-one"),
            "News One",
            "https://stream.example.test/$secret",
            PlaylistEntryKind.Live,
            epgChannelId = rawChannel,
        )
        assertIs<EpgMatchResult.Matched>(adapter.matchChannel(guide, entry).result)

        val otherGuide = guide("source-one", "feed-two")
        adapter.refresh(
            otherGuide,
            flowOf(EpgBatch(listOf(channel.copy(displayNames = listOf("Other"))), listOf(open), 1, 1)),
            anchor,
        )
        assertNotNull(adapter.snapshotInfo(otherGuide))
        assertTrue(adapter.remove(guide))
        assertNull(adapter.snapshotInfo(guide))
        assertNotNull(adapter.snapshotInfo(otherGuide))

        val reopened = DurableCatalogEpgStore(GuideOnlyCatalogStore(backend.reopen()))
        assertNotNull(reopened.snapshotInfo(otherGuide))
        val rendering = listOf(refresh, projection, rows.first().locator, info).joinToString()
        assertFalse(rendering.contains(secret))
    }

    @Test
    fun failedCancelledAndSupersededRefreshesPreservePublishedGuide() = runTest {
        val backend = ContractInMemoryDurableGuideStore()
        val adapter = DurableCatalogEpgStore(GuideOnlyCatalogStore(backend))
        val guide = guide("race-source", "race-feed")
        val channelId = EpgChannelId("channel")
        val channel = EpgChannel(channelId, listOf("Channel"))
        val old = EpgBatch(listOf(channel), listOf(programme(channelId, 1_000, 2_000, "Old")), 1, 1)
        adapter.refresh(guide, flowOf(old), instant(1_500))
        val revision = assertNotNull(adapter.snapshotInfo(guide)).revision

        assertFailsWith<CancellationException> {
            adapter.refresh(
                guide,
                flow {
                    emit(old.copy(programmes = listOf(programme(channelId, 2_000, 3_000, "Cancelled"))))
                    throw CancellationException("cancel")
                },
                instant(1_500),
            )
        }
        assertEquals(revision, adapter.snapshotInfo(guide)?.revision)

        assertFailsWith<EpgStoreException.RefreshFailed> {
            adapter.refresh(
                guide,
                flow {
                    emit(old)
                    error("upstream-secret")
                },
                instant(1_500),
            )
        }
        assertEquals(revision, adapter.snapshotInfo(guide)?.revision)

        val staged = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val first = async {
            adapter.refresh(
                guide,
                flow {
                    emit(old.copy(programmes = listOf(programme(channelId, 3_000, 4_000, "Stale"))))
                    staged.complete(Unit)
                    release.await()
                },
                instant(1_500),
            )
        }
        staged.await()
        val latest = adapter.refresh(
            guide,
            flowOf(old.copy(programmes = listOf(programme(channelId, 4_000, 5_000, "Latest")))),
            instant(1_500),
        )
        release.complete(Unit)
        assertFalse(first.await().committed)
        assertTrue(latest.committed)
        assertEquals("Latest", adapter.visibleWindow(guide, listOf(channelId), instant(0), instant(6_000))
            .channels.single().programmes.single().title)

        assertFailsWith<EpgStoreException.EmptyRefresh> {
            adapter.refresh(guide, flowOf(EpgBatch(emptyList(), emptyList(), 0, 0)), instant(1_500))
        }
        assertNotNull(adapter.snapshotInfo(guide))
    }

    @Test
    fun boundedWindowPruneAndOwnedClose() = runTest {
        val backend = ContractInMemoryDurableGuideStore()
        val catalog = GuideOnlyCatalogStore(backend)
        val adapter = DurableCatalogEpgStore(
            catalog,
            retention = EpgRetentionPolicy(keepPastSeconds = 0, keepFutureSeconds = 100),
            limits = com.getair.iptv.EpgStoreLimits(maxProgrammesPerWindowQuery = 1),
            ownsCatalogStore = true,
        )
        val guide = guide("bounded-source", "bounded-feed")
        val channelId = EpgChannelId("bounded-channel")
        adapter.refresh(
            guide,
            flowOf(
                EpgBatch(
                    listOf(EpgChannel(channelId, listOf("Bounded"))),
                    listOf(
                        programme(channelId, 1_000, 1_500, "First"),
                        programme(channelId, 2_000, 2_500, "Second"),
                    ),
                    1,
                    2,
                ),
            ),
            instant(1_000),
        )
        val window = adapter.visibleWindow(guide, listOf(channelId), instant(0), instant(3_000))
        assertTrue(window.truncated)
        assertEquals(1, window.channels.single().programmes.size)
        val pruned = adapter.prune(guide, instant(2_000))
        assertTrue(pruned.existed)
        assertEquals(1, pruned.removedProgrammes)
        assertEquals(1, pruned.remainingProgrammes)
        adapter.close()
        adapter.close()
        assertTrue(catalog.closed)
    }
}

private fun guide(source: String, feed: String) = EpgGuideKey(SourceId(source), EpgFeedId(feed))

private fun programme(
    channel: EpgChannelId,
    start: Long,
    end: Long?,
    title: String,
) = EpgProgramme(channel, instant(start), end?.let(::instant), title)

private fun instant(value: Long): Instant = Instant.fromEpochMilliseconds(value)

private class GuideOnlyCatalogStore(
    override val guides: DurableGuideStore,
) : DurableCatalogStore {
    var closed = false
    override suspend fun beginRefresh(sourceId: LocalSourceId): CatalogGeneration = unsupported()
    override suspend fun stageCatalogBatch(generation: CatalogGeneration, catalog: DurableCatalogKey, items: List<DurableCatalogItem>) = unsupported()
    override suspend fun stageGuideBatch(
        generation: CatalogGeneration,
        channels: List<EpgChannel>,
        programmes: List<DurableGuideProgramme>,
    ) = unsupported()
    override suspend fun activate(generation: CatalogGeneration, expected: CatalogGenerationCounts): CatalogSourceStatus = unsupported()
    override suspend fun sourceStatus(sourceId: LocalSourceId): CatalogSourceStatus? = unsupported()
    override suspend fun catalogPage(sourceId: LocalSourceId, catalog: DurableCatalogKey, afterSortOrder: Long?, limit: Int): DurableCatalogPage = unsupported()
    override suspend fun channelPage(sourceId: LocalSourceId, afterSortOrder: Long?, limit: Int): DurableChannelPage = unsupported()
    override suspend fun guideWindow(
        sourceId: LocalSourceId,
        channelId: EpgChannelId,
        from: Instant,
        until: Instant,
        limit: Int,
    ): List<DurableGuideProgramme> = unsupported()
    override suspend fun nowNext(sourceId: LocalSourceId, channelId: EpgChannelId, at: Instant): com.getair.iptv.model.EpgNowNext = unsupported()
    override suspend fun deleteSource(sourceId: LocalSourceId) = unsupported()
    override suspend fun cleanupUnreachable(maxRows: Int): CatalogCleanupResult = unsupported()
    override fun close() { closed = true }
    private fun unsupported(): Nothing = error("Legacy catalog path is unsupported")
}
