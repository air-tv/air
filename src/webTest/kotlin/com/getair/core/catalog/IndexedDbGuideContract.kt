package com.getair.core.catalog

import kotlinx.datetime.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal suspend fun verifyIndexedDbGuideContract(
    databaseNamePrefix: String,
    open: suspend (String, () -> Long) -> DurableCatalogStore,
) {
    class Clock(var nowMillis: Long = 1_000_000L)
    data class Entry(
        val databaseName: String,
        val clock: Clock,
        var catalog: DurableCatalogStore,
    )

    var nextDatabase = 0
    val entries = mutableMapOf<DurableGuideStore, Entry>()
    val factory = object : DurableGuideStoreContractFactory {
        override suspend fun create(): DurableGuideStore {
            val databaseName = "$databaseNamePrefix-${nextDatabase++}"
            val clock = Clock()
            val catalog = open(databaseName) { clock.nowMillis }
            val guide = catalog.guides
            entries[guide] = Entry(databaseName, clock, catalog)
            return guide
        }

        override suspend fun reopen(store: DurableGuideStore): DurableGuideStore {
            val entry = entries.remove(store) ?: error("Unknown IndexedDB guide store")
            entry.catalog.close()
            val catalog = open(entry.databaseName) { entry.clock.nowMillis }
            entry.catalog = catalog
            val guide = catalog.guides
            entries[guide] = entry
            return guide
        }

        override suspend fun advanceTimeBy(store: DurableGuideStore, milliseconds: Long) {
            val entry = entries[store] ?: error("Unknown IndexedDB guide store")
            entry.clock.nowMillis += milliseconds
        }
    }

    try {
        verifyDurableGuideStoreContract(factory)
    } finally {
        entries.values.map { it.catalog }.distinct().forEach(DurableCatalogStore::close)
    }
}

internal suspend fun verifyIndexedDbGuideCrossInstance(
    databaseName: String,
    open: suspend (String, () -> Long) -> DurableCatalogStore,
) {
    var nowMillis = 2_000_000L
    val firstCatalog = open(databaseName) { nowMillis }
    val secondCatalog = open(databaseName) { nowMillis }
    try {
        val source = DurableGuideSourceKey("a".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val key = DurableGuideKey(source, DurableGuideFeedId("primary"))
        val channelKey = DurableGuideChannelKey("b".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val retention = DurableGuideRetention(
            Instant.fromEpochMilliseconds(1_000),
            Instant.fromEpochMilliseconds(0),
            Instant.fromEpochMilliseconds(10_000),
        )
        val firstStore = firstCatalog.guides
        val secondStore = secondCatalog.guides
        val generation = firstStore.beginRefresh(key, retention)
        firstStore.stage(
            generation,
            channels = listOf(DurableGuideChannelRecord(channelKey, listOf("Channel"))),
            programmes = listOf(programme(channelKey, 1_000, "first")),
        )
        val firstSnapshot = (firstStore.activate(generation, DurableGuideCounts(1, 1)) as
            DurableGuideActivation.Published).snapshot
        val firstLease = assertNotNull(firstStore.acquire(firstSnapshot))

        val observed = assertNotNull(secondStore.snapshot(key))
        assertEquals(firstSnapshot.revision, observed.revision)
        val secondLease = assertNotNull(secondStore.acquire(observed))
        assertEquals("first", secondStore.programmeSearchRows(secondLease, limit = 1).rows.single().title)

        val replacement = secondStore.beginRefresh(key, retention)
        secondStore.stage(
            replacement,
            channels = listOf(DurableGuideChannelRecord(channelKey, listOf("Channel"))),
            programmes = listOf(programme(channelKey, 2_000, "second")),
        )
        val secondSnapshot = (secondStore.activate(replacement, DurableGuideCounts(1, 1)) as
            DurableGuideActivation.Published).snapshot
        assertEquals(firstSnapshot.revision + 1, secondSnapshot.revision)
        assertEquals(secondSnapshot.revision, assertNotNull(firstStore.snapshot(key)).revision)
        assertEquals("first", firstStore.programmeSearchRows(firstLease, limit = 1).rows.single().title)

        val raw = (firstStore as IndexedDbGuideTestAccess).rawGuideRecordsForTest().joinToString("\n")
        listOf("playbackUrl", "streamUrl", "headers", "password", "username", "credential", "artworkBytes")
            .forEach { forbidden -> assertFalse(raw.contains("\"$forbidden\"", ignoreCase = true), forbidden) }
        assertTrue(raw.length < 128 * 1_024)

        firstStore.release(firstLease)
        secondStore.release(secondLease)
        nowMillis += firstStore.leaseIdleTimeoutMillis + 1
    } finally {
        firstCatalog.close()
        secondCatalog.close()
    }
}

internal suspend fun verifyIndexedDbGuideStartupCleanup(
    databaseName: String,
    openWithoutGuideCleanup: suspend (String, () -> Long) -> DurableCatalogStore,
    openNormally: suspend (String) -> DurableCatalogStore,
) {
    val nowMillis = 1_000_000L
    val firstCatalog = openWithoutGuideCleanup(databaseName) { nowMillis }
    val source = DurableGuideSourceKey("c".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
    val key = DurableGuideKey(source, DurableGuideFeedId("startup"))
    val channelKey = DurableGuideChannelKey("d".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
    val retention = DurableGuideRetention(
        Instant.fromEpochMilliseconds(1_000),
        Instant.fromEpochMilliseconds(0),
        Instant.fromEpochMilliseconds(10_000),
    )
    val generation = firstCatalog.guides.beginRefresh(key, retention)
    firstCatalog.guides.stage(
        generation,
        listOf(DurableGuideChannelRecord(channelKey, listOf("Startup orphan"))),
        listOf(programme(channelKey, 1_000, "startup-orphan-title")),
    )
    assertTrue(firstCatalog.guides.abandon(generation))
    firstCatalog.close()
    val reopened = openNormally(databaseName)
    try {
        val raw = (reopened.guides as IndexedDbGuideTestAccess).rawGuideRecordsForTest().joinToString("\n")
        assertFalse(raw.contains("startup-orphan-title"))
        assertFalse(raw.contains("Startup orphan"))
    } finally {
        reopened.close()
    }
}

internal suspend fun verifyIndexedDbGuideReviewRegressions(
    databaseName: String,
    open: suspend (String, () -> Long) -> DurableCatalogStore,
) {
    var nowMillis = 3_000_000L
    val catalog = open(databaseName) { nowMillis }
    val store = catalog.guides
    try {
        val source = DurableGuideSourceKey("e".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val channelA = DurableGuideChannelKey("f".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val channelB = DurableGuideChannelKey("1".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val emptyChannel = DurableGuideChannelKey("2".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val retention = DurableGuideRetention(
            Instant.fromEpochMilliseconds(500_000),
            Instant.fromEpochMilliseconds(0),
            Instant.fromEpochMilliseconds(1_000_000),
        )

        // Source deletion and revival fence every writer token from the old source epoch.
        val fencedKey = DurableGuideKey(source, DurableGuideFeedId("fenced"))
        val fenced = store.beginRefresh(fencedKey, retention)
        store.stage(
            fenced,
            listOf(DurableGuideChannelRecord(channelA, listOf("Fenced"))),
            listOf(programme(channelA, 1_000, "fenced")),
        )
        assertIs<DurableGuideSourceDeleteResult.Deleted>(
            store.deleteSource(source, store.sourceSnapshot(source).token),
        )
        assertFalse(store.renewGeneration(fenced))
        assertFailsWith<DurableGuideStoreException.Stale> { store.stage(fenced) }
        assertIs<DurableGuideActivation.Superseded>(store.activate(fenced, DurableGuideCounts(1, 1)))
        val revived = store.beginRefresh(fencedKey, retention)
        assertFalse(store.renewGeneration(fenced))
        assertFailsWith<DurableGuideStoreException.Stale> { store.stage(fenced) }
        assertTrue(store.abandon(revived))

        // Oversized batches poison without serializing their records; retention rejection is non-poisoning.
        val oversized = store.beginRefresh(
            DurableGuideKey(source, DurableGuideFeedId("oversized-regression")),
            retention,
        )
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.stage(
                oversized,
                channels = List(DurableGuideLimits.MAX_BATCH_ITEMS + 1) {
                    DurableGuideChannelRecord(channelA, listOf("Oversized"))
                },
            )
        }
        assertFalse(store.renewGeneration(oversized))
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.activate(oversized, DurableGuideCounts(0, 0))
        }
        assertTrue(store.abandon(oversized))

        val retentionWriter = store.beginRefresh(
            DurableGuideKey(source, DurableGuideFeedId("retention-regression")),
            retention,
        )
        assertFailsWith<IllegalArgumentException> {
            store.stage(retentionWriter, programmes = listOf(programme(channelA, 1_000_000, "outside")))
        }
        assertTrue(store.renewGeneration(retentionWriter))
        assertTrue(store.abandon(retentionWriter))

        // Exhausted total rows keep later empty channels honest and never fetch one extra row.
        val multiKey = DurableGuideKey(source, DurableGuideFeedId("multi-regression"))
        val multiGeneration = store.beginRefresh(multiKey, retention)
        store.stage(
            multiGeneration,
            channels = listOf(
                DurableGuideChannelRecord(channelA, listOf("A")),
                DurableGuideChannelRecord(channelB, listOf("B")),
                DurableGuideChannelRecord(emptyChannel, listOf("Empty")),
            ),
            programmes = listOf(
                programme(channelA, 100_000, "a"),
                programme(channelB, 100_000, "b"),
            ),
        )
        val multiSnapshot = (store.activate(multiGeneration, DurableGuideCounts(3, 2)) as
            DurableGuideActivation.Published).snapshot
        val multiLease = assertNotNull(store.acquire(multiSnapshot))
        val multi = store.multiChannelWindow(
            multiLease,
            DurableGuideMultiChannelWindowRequest(
                listOf(channelA, emptyChannel, channelB),
                Instant.fromEpochMilliseconds(99_000),
                Instant.fromEpochMilliseconds(102_000),
                perChannelLimit = 1,
                totalLimit = 1,
            ),
        )
        assertEquals(listOf(1, 0, 0), multi.channels.map { it.programmes.size })
        assertEquals(listOf(false, false, true), multi.channels.map { it.truncated })
        store.release(multiLease)

        // The finite-start index jumps over dense historic non-overlaps.
        val windowKey = DurableGuideKey(source, DurableGuideFeedId("window-regression"))
        val windowGeneration = store.beginRefresh(windowKey, retention)
        store.stage(windowGeneration, channels = listOf(DurableGuideChannelRecord(channelA, listOf("Window"))))
        repeat(3) { batch ->
            store.stage(
                windowGeneration,
                programmes = List(200) { index ->
                    val start = (batch * 200L + index) * 500L
                    programme(channelA, start, "historic-$batch-$index", durationMillis = 100)
                },
            )
        }
        val windowSnapshot = (store.activate(windowGeneration, DurableGuideCounts(1, 600)) as
            DurableGuideActivation.Published).snapshot
        val windowLease = assertNotNull(store.acquire(windowSnapshot))
        val emptyWindow = store.window(
            windowLease,
            channelA,
            Instant.fromEpochMilliseconds(500_000),
            Instant.fromEpochMilliseconds(501_000),
            limit = 10,
        )
        assertTrue(emptyWindow.programmes.isEmpty())
        assertFalse(emptyWindow.truncated)
        store.release(windowLease)

        val edgeRetention = DurableGuideRetention(
            Instant.fromEpochMilliseconds(1_000),
            Instant.fromEpochMilliseconds(1_000),
            Instant.fromEpochMilliseconds(10_000),
        )
        val edgeKey = DurableGuideKey(source, DurableGuideFeedId("left-edge-regression"))
        val edgeGeneration = store.beginRefresh(edgeKey, edgeRetention)
        store.stage(
            edgeGeneration,
            listOf(DurableGuideChannelRecord(channelA, listOf("Left edge"))),
            listOf(programme(channelA, 500, "overlaps-left-edge", durationMillis = 1_000)),
        )
        val edgeSnapshot = (store.activate(edgeGeneration, DurableGuideCounts(1, 1)) as
            DurableGuideActivation.Published).snapshot
        val edgeLease = assertNotNull(store.acquire(edgeSnapshot))
        assertEquals(
            "overlaps-left-edge",
            store.window(
                edgeLease,
                channelA,
                Instant.fromEpochMilliseconds(1_000),
                Instant.fromEpochMilliseconds(1_100),
                limit = 10,
            ).programmes.single().title,
        )
        store.release(edgeLease)

        // maxRows=1 consumes at most one expired lease or payload row per pass.
        val cleanupKey = DurableGuideKey(source, DurableGuideFeedId("cleanup-regression"))
        val cleanupGeneration = store.beginRefresh(cleanupKey, retention)
        store.stage(
            cleanupGeneration,
            listOf(DurableGuideChannelRecord(channelA, listOf("Cleanup"))),
            listOf(programme(channelA, 10_000, "cleanup-old")),
        )
        val cleanupSnapshot = (store.activate(cleanupGeneration, DurableGuideCounts(1, 1)) as
            DurableGuideActivation.Published).snapshot
        repeat(16) { assertNotNull(store.acquire(cleanupSnapshot)) }
        val replacement = store.beginRefresh(cleanupKey, retention)
        store.stage(
            replacement,
            listOf(DurableGuideChannelRecord(channelA, listOf("Cleanup replacement"))),
            listOf(programme(channelA, 20_000, "cleanup-new")),
        )
        store.activate(replacement, DurableGuideCounts(1, 1))
        nowMillis += store.leaseIdleTimeoutMillis + 1
        var passes = 0
        var removedPayloadRows = 0
        var cleanup: DurableGuideCleanupResult
        do {
            cleanup = store.cleanupUnreachable(1)
            assertTrue(cleanup.removedRows in 0..1)
            removedPayloadRows += cleanup.removedRows
            passes += 1
        } while (cleanup.hasMore && passes < 64)
        assertTrue(passes >= 18)
        assertEquals(2, removedPayloadRows)
        assertFalse(cleanup.hasMore)
    } finally {
        catalog.close()
    }
}

private fun programme(
    channelKey: DurableGuideChannelKey,
    startMillis: Long,
    title: String,
    durationMillis: Long = 1_000,
): DurableGuideProgrammeRecord = DurableGuideProgrammeRecord(
    channelKey = channelKey,
    start = Instant.fromEpochMilliseconds(startMillis),
    end = Instant.fromEpochMilliseconds(startMillis + durationMillis),
    title = title,
)
