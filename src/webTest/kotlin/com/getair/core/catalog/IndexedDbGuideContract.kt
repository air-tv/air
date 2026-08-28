package com.getair.core.catalog

import kotlinx.datetime.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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

private fun programme(
    channelKey: DurableGuideChannelKey,
    startMillis: Long,
    title: String,
): DurableGuideProgrammeRecord = DurableGuideProgrammeRecord(
    channelKey = channelKey,
    start = Instant.fromEpochMilliseconds(startMillis),
    end = Instant.fromEpochMilliseconds(startMillis + 1_000),
    title = title,
)
