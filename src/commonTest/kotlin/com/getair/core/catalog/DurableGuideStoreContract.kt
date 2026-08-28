package com.getair.core.catalog

import kotlinx.datetime.Instant
import kotlin.math.min
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Backend tests invoke this factory to run the same durable-guide behavior contract. */
internal interface DurableGuideStoreContractFactory {
    suspend fun create(): DurableGuideStore

    suspend fun reopen(store: DurableGuideStore): DurableGuideStore

    /** Advances the backend's injected lease clock without sleeping. */
    suspend fun advanceTimeBy(store: DurableGuideStore, milliseconds: Long)
}

/** Executable storage contract shared by future SQLDelight and IndexedDB adapters. */
internal suspend fun verifyDurableGuideStoreContract(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val source = DurableGuideSourceKey(opaque("source-a"))
    val otherSource = DurableGuideSourceKey(opaque("source-b"))
    val feedA = DurableGuideFeedId("feed-a")
    val feedB = DurableGuideFeedId("feed-b")
    val keyA = DurableGuideKey(source, feedA)
    val keyB = DurableGuideKey(source, feedB)
    val otherKey = DurableGuideKey(otherSource, feedA)
    val channel = DurableGuideChannelKey(opaque("channel-a"))
    val retention = retention(1_000, 0, 10_000)
    assertTrue(store.leaseIdleTimeoutMillis in 1..DurableGuideLimits.MAX_LEASE_IDLE_TIMEOUT_MILLIS)
    assertTrue(
        store.generationIdleTimeoutMillis in 1..DurableGuideLimits.MAX_GENERATION_IDLE_TIMEOUT_MILLIS,
    )

    val discardedProgramme = programme(
        channel,
        "winner-z",
        1_000,
        2_000,
        "discarded",
        description = "discarded-description",
    )
    val canonicalProgramme = programme(
        channel,
        "winner-a",
        1_000,
        2_000,
        "canonical",
        description = "full-description",
    )
    val expectedProgramme = listOf(discardedProgramme, canonicalProgramme)
        .minWith(DurableGuideWinnerPolicy::compareProgrammes)
    val first = store.beginRefresh(keyA, retention)
    val firstCounts = store.stage(
        first,
        channels = listOf(channel(channel, "Channel A")),
        programmes = listOf(
            discardedProgramme,
        ),
    )
    assertEquals(DurableGuideCounts(1, 1), firstCounts)
    val dedupedCounts = store.stage(
        first,
        programmes = listOf(
            canonicalProgramme,
            programme(channel, "open", 3_000, null, "open-ended"),
        ),
    )
    assertEquals(DurableGuideCounts(1, 2), dedupedCounts)
    val published = store.activate(first, DurableGuideCounts(1, 2)) as DurableGuideActivation.Published
    assertEquals(1, published.snapshot.revision)
    assertEquals(2, published.snapshot.counts.programmes)
    assertEquals(retention, published.snapshot.retention)

    // Same source, different feed and different source, same feed remain isolated.
    publish(store, keyB, retention, channel, "feed-b")
    publish(store, otherKey, retention, channel, "other-source")
    assertEquals(expectedProgramme.title, firstSearchTitle(store, keyA))
    assertEquals("feed-b", firstSearchTitle(store, keyB))
    assertEquals("other-source", firstSearchTitle(store, otherKey))

    val sourceState = store.sourceSnapshot(source)
    val sourceSnapshots = store.snapshots(sourceState, limit = 1)
    assertEquals(1, sourceSnapshots.snapshots.size)
    assertNotNull(sourceSnapshots.nextCursor)
    val sourceSnapshotsTail = store.snapshots(sourceState, sourceSnapshots.nextCursor, 1)
    assertEquals(2, (sourceSnapshots.snapshots + sourceSnapshotsTail.snapshots).size)

    // A lease pins pages and opaque locators to exactly one revision.
    val oldSnapshot = assertNotNull(store.snapshot(keyA))
    val oldLease = assertNotNull(store.acquire(oldSnapshot))
    val firstSearchPage = store.programmeSearchRows(oldLease, limit = 1)
    val canonicalRow = firstSearchPage.rows.single()
    assertEquals(expectedProgramme.title, canonicalRow.title)
    assertNotNull(firstSearchPage.nextCursor)
    val secondSearchPage = store.programmeSearchRows(oldLease, firstSearchPage.nextCursor, 1)
    assertEquals("open-ended", secondSearchPage.rows.single().title)
    assertNull(secondSearchPage.nextCursor)
    assertEquals(expectedProgramme.description, store.programme(oldLease, canonicalRow.locator)?.description)
    assertEquals("S01E02", store.programme(oldLease, canonicalRow.locator)?.episode)
    assertEquals(
        "https://images.example.test/programme.jpg",
        store.programme(oldLease, canonicalRow.locator)?.artworkReference,
    )
    assertNull(store.programme(oldLease, secondSearchPage.rows.single().locator)?.end)
    assertEquals(Instant.DISTANT_FUTURE, store.programme(oldLease, secondSearchPage.rows.single().locator)?.effectiveEnd)

    val channels = store.channels(oldLease, limit = 1)
    assertEquals("Channel A", channels.channels.single().displayNames.single())
    assertEquals("https://images.example.test/channel.jpg", channels.channels.single().artworkReference)
    val window = store.window(oldLease, channel, instant(500), instant(5_000), limit = 1)
    assertEquals(1, window.programmes.size)
    assertTrue(window.truncated)
    assertNotNull(window.nextCursor)
    val nowNext = store.nowNext(oldLease, channel, instant(1_500))
    assertEquals(expectedProgramme.title, nowNext.current?.title)
    assertEquals("open-ended", nowNext.next?.title)

    // A newer generation wins even when the older generation activates later.
    val stale = store.beginRefresh(keyA, retention)
    val latest = store.beginRefresh(keyA, retention)
    store.stage(latest, channels = listOf(channel(channel, "Latest")), programmes = listOf(programme(channel, "new", 5_000, 6_000, "latest")))
    assertTrue(store.activate(stale, DurableGuideCounts(0, 0)) is DurableGuideActivation.Superseded)
    val latestActivation = store.activate(latest, DurableGuideCounts(1, 1)) as DurableGuideActivation.Published
    assertEquals(2, latestActivation.snapshot.revision)
    assertEquals(expectedProgramme.title, store.programme(oldLease, canonicalRow.locator)?.title)
    val latestLease = assertNotNull(store.acquire(latestActivation.snapshot))
    val latestLocator = store.programmeSearchRows(latestLease, limit = 1).rows.single().locator
    assertNull(store.programme(latestLease, canonicalRow.locator))
    assertTrue(store.renew(oldLease))
    store.release(oldLease)
    assertFalse(store.renew(oldLease))
    assertNull(store.programme(oldLease, canonicalRow.locator))

    // Locators are also bound to a backend instance, not just matching numbers.
    val otherStore = factory.create()
    assertFailsWith<DurableGuideStoreException.Stale> {
        otherStore.stage(first, channels = emptyList(), programmes = emptyList())
    }
    assertNull(otherStore.acquire(oldSnapshot))
    val otherGeneration = otherStore.beginRefresh(keyA, retention)
    otherStore.stage(otherGeneration, channels = listOf(channel(channel, "Other")), programmes = listOf(programme(channel, "other", 5_000, 6_000, "other")))
    val otherSnapshot = (otherStore.activate(otherGeneration, DurableGuideCounts(1, 1)) as DurableGuideActivation.Published).snapshot
    val otherLease = assertNotNull(otherStore.acquire(otherSnapshot))
    val otherLocator = otherStore.programmeSearchRows(otherLease, limit = 1).rows.single().locator
    assertNull(store.programme(latestLease, otherLocator))
    assertFalse(otherStore.renew(latestLease))
    assertNull(otherStore.programme(latestLease, latestLocator))
    assertEquals("latest", store.programme(latestLease, latestLocator)?.title)
    val originalSourceState = store.sourceSnapshot(source)
    val originalCursor = assertNotNull(store.snapshots(originalSourceState, limit = 1).nextCursor)
    assertFailsWith<DurableGuideStoreException.Stale> {
        otherStore.snapshots(otherStore.sourceSnapshot(source), originalCursor, 1)
    }
    assertEquals(1, store.snapshots(originalSourceState, originalCursor, 1).snapshots.size)

    // Abandoned pins expire without sleeping and cannot block bounded cleanup forever.
    val abandoned = assertNotNull(store.acquire(latestActivation.snapshot))
    factory.advanceTimeBy(store, store.leaseIdleTimeoutMillis + 1)
    assertFalse(store.renew(abandoned))
    assertNull(store.programme(abandoned, latestLocator))

    val oversized = store.beginRefresh(guideKey("oversized", "main"), retention)
    assertFailsWith<DurableGuideStoreException.Limit> {
        store.stage(oversized, channels = List(DurableGuideLimits.MAX_BATCH_ITEMS + 1) { channel(channel, "Too many") })
    }
    assertTrue(store.abandon(oversized))
    val credential = "never-persist-this"
    val unsafeArtwork = assertFailsWith<IllegalArgumentException> {
        DurableGuideChannelRecord(
            channel,
            listOf("Unsafe"),
            artworkReference = "https://images.example.test/image.jpg?token=$credential",
        )
    }
    assertFalse(unsafeArtwork.message.orEmpty().contains(credential))
    listOf(
        "https://images.example.test/image.jpg?%74oken=$credential",
        "https://images.example.test/image.jpg?signature=$credential",
        "https:///image.jpg",
        "https://images.example.test/image.jpg#fragment",
    ).forEach { unsafeReference ->
        val failure = assertFailsWith<IllegalArgumentException> {
            DurableGuideChannelRecord(
                channel,
                listOf("Unsafe"),
                artworkReference = unsafeReference,
            )
        }
        assertFalse(failure.message.orEmpty().contains(credential))
    }

    // Conditional deletion preserves newer state; successful deletion increments a tombstone revision.
    assertTrue(
        store.deleteGuide(keyA, expectedRevision = 1, expectedMutationEpoch = latestActivation.snapshot.mutationEpoch)
            is DurableGuideDeleteResult.Superseded,
    )
    val deleted = store.deleteGuide(
        keyA,
        expectedRevision = 2,
        expectedMutationEpoch = latestActivation.snapshot.mutationEpoch,
    ) as DurableGuideDeleteResult.Deleted
    assertEquals(3, deleted.revision)
    assertNull(store.snapshot(keyA))
    var cleanupPasses = 0
    var cleanup = DurableGuideCleanupResult(0, true)
    do {
        cleanup = store.cleanupUnreachable(1)
        assertTrue(cleanup.removedRows <= 1)
        cleanupPasses += 1
    } while (cleanup.hasMore && cleanupPasses < 32)
    assertTrue(cleanupPasses > 1)
    assertFalse(cleanup.hasMore)
    val afterDelete = store.beginRefresh(keyA, retention)
    store.stage(
        afterDelete,
        channels = listOf(channel(channel, "Restored")),
        programmes = listOf(
            programme(channel, "old", 1_000, 2_000, "old"),
            programme(channel, "restored", 7_000, 8_000, "restored"),
        ),
    )
    val restored = store.activate(afterDelete, DurableGuideCounts(1, 2)) as DurableGuideActivation.Published
    assertEquals(4, restored.snapshot.revision)
    assertTrue(
        store.prune(
            keyA,
            expectedRevision = 3,
            expectedMutationEpoch = restored.snapshot.mutationEpoch,
            retention = retention(1_000, 7_500, 8_500),
        )
            is DurableGuidePruneResult.Superseded,
    )
    val pruned = store.prune(
        keyA,
        expectedRevision = 4,
        expectedMutationEpoch = restored.snapshot.mutationEpoch,
        retention = retention(1_000, 7_500, 8_500),
    )
        as DurableGuidePruneResult.Published
    assertEquals(5, pruned.snapshot.revision)
    assertEquals(1, pruned.snapshot.counts.programmes)

    // Exact source revision conditions prevent partial source deletion.
    val staleSourceToken = store.sourceSnapshot(source).token
    val feedBRefresh = store.beginRefresh(keyB, retention)
    store.stage(feedBRefresh, channels = listOf(channel(channel, "Feed B2")), programmes = listOf(programme(channel, "b2", 8_000, 9_000, "feed-b2")))
    store.activate(feedBRefresh, DurableGuideCounts(1, 1))
    val sourceDeleteMismatch = store.deleteSource(source, staleSourceToken)
    assertTrue(sourceDeleteMismatch is DurableGuideSourceDeleteResult.Superseded)
    val sourceDelete = store.deleteSource(source, store.sourceSnapshot(source).token)
    assertTrue(sourceDelete is DurableGuideSourceDeleteResult.Deleted)
    assertTrue(store.snapshots(store.sourceSnapshot(source), limit = 10).snapshots.isEmpty())

    // Compact projections and public rendering must not expose full metadata/provider-like strings.
    val rendered = listOf(
        published.snapshot,
        canonicalRow,
        canonicalRow.locator,
        channel,
        programme(channel, "render", 1_000, 2_000, "secret-title", description = "secret-description"),
    ).joinToString()
    assertFalse(rendered.contains("secret-title"))
    assertFalse(rendered.contains("secret-description"))
    assertFalse(rendered.contains("channel-a"))

    store.release(latestLease)
    otherStore.release(otherLease)

    verifyGenerationLifecycle(factory)
    verifyStalePagingSemantics(factory)
    verifyLeaseCleanupAndAdmission(factory)
    verifyPruneMutationEpoch(factory)
    verifyChannelWinnersAndMultiWindow(factory)
    verifyDeleteCleanupRestartMonotonicity(factory)
    verifyHardLimits()
}

private suspend fun verifyGenerationLifecycle(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val key = guideKey("lifecycle", "main")
    val channelKey = channelKey("lifecycle")
    publishSnapshot(store, key, channelKey, listOf(programme(channelKey, "base", 1_000, 2_000, "base")))

    val live = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(live, listOf(channel(channelKey, "new")), listOf(programme(channelKey, "new", 2_000, 3_000, "new")))
    assertEquals(DurableGuideCleanupResult(0, false), store.cleanupUnreachable(1))
    assertEquals("base", firstSearchTitle(store, key))
    assertTrue(store.renewGeneration(live))
    assertTrue(store.abandon(live))
    assertFalse(store.abandon(live))
    assertEquals("base", firstSearchTitle(store, key))
    var cleanup = store.cleanupUnreachable(1)
    assertEquals(1, cleanup.removedRows)
    while (cleanup.hasMore) cleanup = store.cleanupUnreachable(1)

    val poisoned = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(
        poisoned,
        listOf(channel(channelKey, "poisoned")),
        listOf(programme(channelKey, "poisoned", 2_000, 3_000, "poisoned")),
    )
    assertFailsWith<DurableGuideStoreException.Limit> {
        store.stage(
            poisoned,
            channels = List(DurableGuideLimits.MAX_BATCH_ITEMS + 1) { channel(channelKey, "duplicate") },
        )
    }
    assertFalse(store.renewGeneration(poisoned))
    assertFailsWith<DurableGuideStoreException.Limit> {
        store.activate(poisoned, DurableGuideCounts(1, 1))
    }
    var poisonedRemoved = 0
    do {
        cleanup = store.cleanupUnreachable(1)
        poisonedRemoved += cleanup.removedRows
    } while (cleanup.hasMore)
    assertEquals(2, poisonedRemoved)
    assertEquals("base", firstSearchTitle(store, key))

    val crashed = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(crashed, listOf(channel(channelKey, "crash")), listOf(programme(channelKey, "crash", 3_000, 4_000, "crash")))
    factory.advanceTimeBy(store, store.generationIdleTimeoutMillis + 1)
    val reopened = factory.reopen(store)
    cleanup = reopened.cleanupUnreachable(1)
    assertEquals(0, cleanup.removedRows)
    assertTrue(cleanup.hasMore)
    var crashedRemoved = 0
    while (cleanup.hasMore) {
        cleanup = reopened.cleanupUnreachable(1)
        crashedRemoved += cleanup.removedRows
    }
    assertEquals(2, crashedRemoved)
    assertEquals("base", firstSearchTitle(reopened, key))

    val empty = reopened.beginRefresh(key, retention(1_000, 0, 10_000))
    assertFailsWith<DurableGuideStoreException.Limit> {
        reopened.activate(empty, DurableGuideCounts(0, 0))
    }
    assertTrue(reopened.abandon(empty))
    assertEquals("base", firstSearchTitle(reopened, key))
    assertFailsWith<DurableGuideStoreException.Limit> { reopened.cleanupUnreachable(0) }
    assertFailsWith<DurableGuideStoreException.Limit> {
        reopened.cleanupUnreachable(DurableGuideLimits.MAX_CLEANUP_ROWS + 1)
    }
    reopened.cleanupUnreachable(DurableGuideLimits.MAX_CLEANUP_ROWS)
    val bounded = reopened.beginRefresh(key, retention(1_000, 0, 10_000))
    reopened.stage(
        bounded,
        listOf(channel(channelKey, "bounded")),
        listOf(programme(channelKey, "bounded", 4_000, 5_000, "bounded")),
    )
    assertTrue(reopened.abandon(bounded))
    val exactBound = reopened.cleanupUnreachable(DurableGuideLimits.MAX_CLEANUP_ROWS)
    assertEquals(2, exactBound.removedRows)
    assertFalse(exactBound.hasMore)

    val deleteStore = factory.create()
    val deleteSource = sourceKey("delete-source")
    val activeKey = DurableGuideKey(deleteSource, DurableGuideFeedId("active"))
    publishSnapshot(
        deleteStore,
        activeKey,
        channelKey,
        listOf(programme(channelKey, "active", 1_000, 2_000, "active")),
    )
    val stagedOnlyKey = DurableGuideKey(deleteSource, DurableGuideFeedId("staged-only"))
    val stagedOnly = deleteStore.beginRefresh(stagedOnlyKey, retention(1_000, 0, 10_000))
    deleteStore.stage(
        stagedOnly,
        listOf(channel(channelKey, "staged")),
        listOf(programme(channelKey, "staged", 2_000, 3_000, "staged")),
    )
    val deleted = deleteStore.deleteSource(deleteSource, deleteStore.sourceSnapshot(deleteSource).token)
        as DurableGuideSourceDeleteResult.Deleted
    assertEquals(1, deleted.activeFeedCount)
    assertEquals(1, deleted.stagedOnlyFeedCount)
}

private suspend fun verifyStalePagingSemantics(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val source = sourceKey("paging")
    val channelKey = channelKey("paging")
    val firstKey = DurableGuideKey(source, DurableGuideFeedId("one"))
    val secondKey = DurableGuideKey(source, DurableGuideFeedId("two"))
    val thirdKey = DurableGuideKey(source, DurableGuideFeedId("three"))
    val firstSnapshot = publishSnapshot(
        store,
        firstKey,
        channelKey,
        listOf(
            programme(channelKey, "one", 1_000, 2_000, "one"),
            programme(channelKey, "two", 2_000, 3_000, "two"),
        ),
    )
    publishSnapshot(store, secondKey, channelKey, listOf(programme(channelKey, "other", 1_000, 2_000, "other")))
    val sourceState = store.sourceSnapshot(source)
    val sourcePage = store.snapshots(sourceState, limit = 1)
    val sourceCursor = assertNotNull(sourcePage.nextCursor)
    publishSnapshot(store, thirdKey, channelKey, listOf(programme(channelKey, "third", 1_000, 2_000, "third")))
    assertFailsWith<DurableGuideStoreException.Stale> {
        store.snapshots(sourceState, sourceCursor, 1)
    }

    val lease = assertNotNull(store.acquire(firstSnapshot))
    val searchCursor = assertNotNull(store.programmeSearchRows(lease, limit = 1).nextCursor)
    assertFailsWith<DurableGuideStoreException.Stale> { store.channels(lease, searchCursor, 1) }
    val windowCursor = assertNotNull(
        store.window(lease, channelKey, instant(0), instant(4_000), limit = 1).nextCursor,
    )
    assertFailsWith<DurableGuideStoreException.Stale> {
        store.window(lease, channelKey, instant(1), instant(4_000), windowCursor, 1)
    }

    val other = factory.create()
    val otherSource = sourceKey("paging-other")
    publishSnapshot(
        other,
        DurableGuideKey(otherSource, DurableGuideFeedId("one")),
        channelKey,
        listOf(programme(channelKey, "other", 1_000, 2_000, "other")),
    )
    assertFailsWith<DurableGuideStoreException.Stale> {
        other.snapshots(other.sourceSnapshot(otherSource), sourceCursor, 1)
    }

    store.release(lease)
    assertFailsWith<DurableGuideStoreException.Stale> { store.channels(lease, limit = 1) }
    assertFailsWith<DurableGuideStoreException.Stale> { store.nowNext(lease, channelKey, instant(1_500)) }
    assertFailsWith<DurableGuideStoreException.Stale> {
        store.window(lease, channelKey, instant(0), instant(4_000), limit = 1)
    }
    assertFailsWith<DurableGuideStoreException.Stale> {
        store.multiChannelWindow(
            lease,
            DurableGuideMultiChannelWindowRequest(listOf(channelKey), instant(0), instant(4_000)),
        )
    }

    val expiring = assertNotNull(store.acquire(assertNotNull(store.snapshot(firstKey))))
    factory.advanceTimeBy(store, store.leaseIdleTimeoutMillis + 1)
    assertFailsWith<DurableGuideStoreException.Stale> { store.channels(expiring, limit = 1) }
    assertFailsWith<DurableGuideStoreException.Stale> { store.nowNext(expiring, channelKey, instant(1_500)) }

    val nowStore = factory.create()
    val nowKey = guideKey("now-next", "main")
    val nowChannel = channelKey("now-next")
    val nowSnapshot = publishSnapshot(
        nowStore,
        nowKey,
        nowChannel,
        listOf(
            programme(nowChannel, "open", 1_000, null, "open"),
            programme(nowChannel, "later", 2_000, 3_000, "later"),
        ),
    )
    val nowLease = assertNotNull(nowStore.acquire(nowSnapshot))
    val openNowNext = nowStore.nowNext(nowLease, nowChannel, instant(1_500))
    assertEquals("open", openNowNext.current?.title)
    assertEquals("later", openNowNext.next?.title)
    nowStore.release(nowLease)
}

private suspend fun verifyLeaseCleanupAndAdmission(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val key = guideKey("lease-cleanup", "main")
    val channelKey = channelKey("lease-cleanup")
    val first = publishSnapshot(
        store,
        key,
        channelKey,
        listOf(programme(channelKey, "first", 1_000, 2_000, "first")),
    )
    val lease = assertNotNull(store.acquire(first))
    val locator = store.programmeSearchRows(lease, limit = 1).rows.single().locator
    publishSnapshot(store, key, channelKey, listOf(programme(channelKey, "second", 2_000, 3_000, "second")))
    val foreign = factory.create()
    factory.advanceTimeBy(foreign, foreign.leaseIdleTimeoutMillis + 1)
    assertNull(foreign.programme(lease, locator))
    assertFailsWith<DurableGuideStoreException.Stale> { foreign.channels(lease, limit = 1) }
    assertEquals("first", store.programme(lease, locator)?.title)
    assertEquals(1, store.channels(lease, limit = 1).channels.size)
    assertEquals(DurableGuideCleanupResult(0, false), store.cleanupUnreachable(1))
    assertEquals("first", store.programme(lease, locator)?.title)
    factory.advanceTimeBy(store, store.leaseIdleTimeoutMillis - 1)
    assertTrue(store.renew(lease))
    assertEquals(DurableGuideCleanupResult(0, false), store.cleanupUnreachable(1))
    store.release(lease)
    assertNull(store.programme(lease, locator))
    assertEquals(1, store.cleanupUnreachable(1).removedRows)

    val current = assertNotNull(store.snapshot(key))
    val leases = List(DurableGuideLimits.MAX_LIVE_LEASES) { assertNotNull(store.acquire(current)) }
    assertFailsWith<DurableGuideStoreException.Limit> { store.acquire(current) }
    factory.advanceTimeBy(store, store.leaseIdleTimeoutMillis + 1)
    val replacement = assertNotNull(store.acquire(current))
    store.release(replacement)
    leases.forEach { store.release(it) }
}

private suspend fun verifyPruneMutationEpoch(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val key = guideKey("prune", "main")
    val channelKey = channelKey("prune")
    val original = publishSnapshot(
        store,
        key,
        channelKey,
        listOf(
            programme(channelKey, "old", 1_000, 2_000, "old"),
            programme(channelKey, "keep", 7_000, 8_000, "keep"),
        ),
    )
    val pending = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(pending, listOf(channel(channelKey, "pending")), listOf(programme(channelKey, "pending", 8_000, 9_000, "pending")))
    assertTrue(
        store.prune(key, original.revision, original.mutationEpoch, retention(1_000, 7_500, 8_500))
            is DurableGuidePruneResult.Superseded,
    )
    assertTrue(
        store.deleteGuide(key, original.revision, original.mutationEpoch)
            is DurableGuideDeleteResult.Superseded,
    )
    val refreshed = assertNotNull(store.snapshot(key))
    val published = store.prune(
        key,
        refreshed.revision,
        refreshed.mutationEpoch,
        retention(1_000, 7_500, 8_500),
    ) as DurableGuidePruneResult.Published
    assertTrue(store.activate(pending, DurableGuideCounts(1, 1)) is DurableGuideActivation.Superseded)
    assertEquals(original.revision + 1, published.snapshot.revision)

    listOf(
        retention(1_000, 7_499, 8_500),
        retention(1_000, 7_500, 8_501),
        retention(1_001, 7_500, 8_500),
    ).forEach { invalid ->
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.prune(
                key,
                published.snapshot.revision,
                published.snapshot.mutationEpoch,
                invalid,
            )
        }
    }
    val unchanged = store.prune(
        key,
        published.snapshot.revision,
        published.snapshot.mutationEpoch,
        retention(1_000, 7_600, 8_400),
    ) as DurableGuidePruneResult.Unchanged
    assertEquals(published.snapshot.revision, unchanged.current.revision)
    assertEquals(published.snapshot.retention, unchanged.current.retention)
    assertTrue(unchanged.current.mutationEpoch > published.snapshot.mutationEpoch)
}

private suspend fun verifyChannelWinnersAndMultiWindow(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val key = guideKey("multi", "main")
    val firstChannel = channelKey("first")
    val emptyChannel = channelKey("empty")
    val thirdChannel = channelKey("third")
    val discardedChannel = channelCandidate(firstChannel, "discarded")
    val canonicalChannel = channelCandidate(firstChannel, "canonical")
    assertNotEquals(discardedChannel.winnerKey, canonicalChannel.winnerKey)
    assertEquals(canonicalChannel.winnerKey, channelCandidate(firstChannel, "canonical").winnerKey)
    val expectedChannel = listOf(discardedChannel, canonicalChannel)
        .minWith(DurableGuideWinnerPolicy::compareChannels)
    val programmeA = programme(firstChannel, "payload-a", 1_000, 2_000, "payload-a")
    val programmeB = programme(firstChannel, "payload-b", 1_000, 2_000, "payload-b")
    assertNotEquals(programmeA.winnerKey, programmeB.winnerKey)
    val expectedProgramme = listOf(programmeA, programmeB)
        .minWith(DurableGuideWinnerPolicy::compareProgrammes)
    val generation = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(
        generation,
        channels = listOf(discardedChannel),
        programmes = listOf(programmeA),
    )
    store.stage(
        generation,
        channels = listOf(
            canonicalChannel,
            channel(emptyChannel, "empty"),
            channel(thirdChannel, "third"),
        ),
        programmes = listOf(
            programmeB,
            programme(firstChannel, "first-b", 2_000, 3_000, "first-b"),
            programme(thirdChannel, "third-a", 1_000, 2_000, "third-a"),
            programme(thirdChannel, "third-b", 2_000, 3_000, "third-b"),
        ),
    )
    val snapshot = (store.activate(generation, DurableGuideCounts(3, 4)) as DurableGuideActivation.Published).snapshot
    val lease = assertNotNull(store.acquire(snapshot))
    assertEquals(
        expectedChannel.displayNames.single(),
        store.channels(lease, limit = 3).channels.first { it.key == firstChannel }.displayNames.single(),
    )
    assertEquals(
        expectedProgramme.title,
        store.window(lease, firstChannel, instant(0), instant(4_000), limit = 10).programmes.first().title,
    )
    val multi = store.multiChannelWindow(
        lease,
        DurableGuideMultiChannelWindowRequest(
            listOf(thirdChannel, emptyChannel, firstChannel),
            instant(0),
            instant(4_000),
            perChannelLimit = 1,
            totalLimit = 2,
        ),
    )
    assertEquals(listOf(thirdChannel, emptyChannel, firstChannel), multi.channels.map { it.channelKey })
    assertEquals(listOf(1, 0, 1), multi.channels.map { it.programmes.size })
    assertTrue(multi.channels.first().truncated)
    assertTrue(multi.channels.last().truncated)
    assertTrue(multi.truncated)
    assertEquals(
        multi.channels.sumOf { window -> window.programmes.sumOf(DurableGuidePayloadSizing::programmeBytes) },
        multi.payloadBytes,
    )
    store.release(lease)

    val reversed = factory.create()
    val reversedGeneration = reversed.beginRefresh(key, retention(1_000, 0, 10_000))
    reversed.stage(
        reversedGeneration,
        channels = listOf(canonicalChannel),
        programmes = listOf(programmeB),
    )
    reversed.stage(
        reversedGeneration,
        channels = listOf(discardedChannel),
        programmes = listOf(programmeA),
    )
    val reversedSnapshot =
        (reversed.activate(reversedGeneration, DurableGuideCounts(1, 1)) as DurableGuideActivation.Published).snapshot
    val reversedLease = assertNotNull(reversed.acquire(reversedSnapshot))
    assertEquals(
        expectedChannel.displayNames.single(),
        reversed.channels(reversedLease, limit = 1).channels.single().displayNames.single(),
    )
    assertEquals(
        expectedProgramme.title,
        reversed.window(reversedLease, firstChannel, instant(0), instant(4_000), limit = 10)
            .programmes.single().title,
    )
    reversed.release(reversedLease)

    val payloadStore = factory.create()
    val payloadKey = guideKey("payload", "main")
    val payloadChannel = channelKey("payload")
    val payloadSnapshot = publishSnapshot(
        payloadStore,
        payloadKey,
        payloadChannel,
        listOf(
            programme(
                payloadChannel,
                "huge",
                1_000,
                2_000,
                "huge",
                description = "x".repeat(DurableGuideLimits.MAX_DESCRIPTION_CHARS),
            ),
        ),
    )
    val payloadLease = assertNotNull(payloadStore.acquire(payloadSnapshot))
    val payloadLimited = payloadStore.multiChannelWindow(
        payloadLease,
        DurableGuideMultiChannelWindowRequest(
            listOf(payloadChannel),
            instant(0),
            instant(3_000),
            payloadByteLimit = 256,
        ),
    )
    assertTrue(payloadLimited.channels.single().programmes.isEmpty())
    assertEquals(0, payloadLimited.payloadBytes)
    assertTrue(payloadLimited.truncated)
    payloadStore.release(payloadLease)
}

private fun verifyHardLimits() {
    assertEquals(
        DurableGuideCounts(
            DurableGuideLimits.MAX_GENERATION_CHANNELS,
            DurableGuideLimits.MAX_GENERATION_PROGRAMMES,
        ),
        DurableGuideCounts(
            DurableGuideLimits.MAX_GENERATION_CHANNELS,
            DurableGuideLimits.MAX_GENERATION_PROGRAMMES,
        ),
    )
    assertFailsWith<IllegalArgumentException> {
        DurableGuideCounts(DurableGuideLimits.MAX_GENERATION_CHANNELS + 1, 0)
    }
    assertFailsWith<IllegalArgumentException> {
        DurableGuideCounts(0, DurableGuideLimits.MAX_GENERATION_PROGRAMMES + 1)
    }
    val channel = channelKey("hard-limit")
    assertFailsWith<IllegalArgumentException> {
        DurableGuideMultiChannelWindowRequest(
            List(DurableGuideLimits.MAX_MULTI_WINDOW_CHANNELS + 1) { index -> channelKey("hard-$index") },
            instant(0),
            instant(1),
        )
    }
    assertFailsWith<IllegalArgumentException> {
        DurableGuideMultiChannelWindowRequest(
            listOf(channel),
            instant(0),
            instant(1),
            totalLimit = DurableGuideLimits.MAX_MULTI_WINDOW_ITEMS + 1,
        )
    }
}

private suspend fun verifyDeleteCleanupRestartMonotonicity(factory: DurableGuideStoreContractFactory) {
    val store = factory.create()
    val key = guideKey("restart", "main")
    val channelKey = channelKey("restart")
    val first = publishSnapshot(
        store,
        key,
        channelKey,
        listOf(programme(channelKey, "first", 1_000, 2_000, "first")),
    )
    val deleted = store.deleteGuide(key, first.revision, first.mutationEpoch) as DurableGuideDeleteResult.Deleted
    var removedRows = 0
    var cleanup: DurableGuideCleanupResult
    do {
        cleanup = store.cleanupUnreachable(1)
        removedRows += cleanup.removedRows
    } while (cleanup.hasMore)
    assertEquals(2, removedRows)

    val reopened = factory.reopen(store)
    val restored = publishSnapshot(
        reopened,
        key,
        channelKey,
        listOf(programme(channelKey, "restored", 2_000, 3_000, "restored")),
    )
    assertTrue(restored.generation > first.generation)
    assertTrue(restored.revision > deleted.revision)
}

private suspend fun publish(
    store: DurableGuideStore,
    key: DurableGuideKey,
    retention: DurableGuideRetention,
    channelKey: DurableGuideChannelKey,
    title: String,
) {
    val generation = store.beginRefresh(key, retention)
    store.stage(
        generation,
        channels = listOf(channel(channelKey, title)),
        programmes = listOf(programme(channelKey, title, 1_000, 2_000, title)),
    )
    assertTrue(store.activate(generation, DurableGuideCounts(1, 1)) is DurableGuideActivation.Published)
}

private suspend fun publishSnapshot(
    store: DurableGuideStore,
    key: DurableGuideKey,
    channelKey: DurableGuideChannelKey,
    programmes: List<DurableGuideProgrammeRecord>,
): DurableGuideSnapshot {
    val generation = store.beginRefresh(key, retention(1_000, 0, 10_000))
    store.stage(generation, listOf(channel(channelKey, "channel")), programmes)
    return (store.activate(
        generation,
        DurableGuideCounts(1, programmes.size.toLong()),
    ) as DurableGuideActivation.Published).snapshot
}

private fun sourceKey(seed: String): DurableGuideSourceKey = DurableGuideSourceKey(opaque(seed))

private fun guideKey(source: String, feed: String): DurableGuideKey =
    DurableGuideKey(sourceKey(source), DurableGuideFeedId(feed))

private fun channelKey(seed: String): DurableGuideChannelKey = DurableGuideChannelKey(opaque(seed))

private fun channelCandidate(
    key: DurableGuideChannelKey,
    name: String,
) = DurableGuideChannelRecord(
    key,
    listOf(name),
    artworkReference = "https://images.example.test/channel.jpg",
)

private suspend fun firstSearchTitle(store: DurableGuideStore, key: DurableGuideKey): String {
    val lease = assertNotNull(store.acquire(assertNotNull(store.snapshot(key))))
    return try {
        store.programmeSearchRows(lease, limit = 1).rows.single().title
    } finally {
        store.release(lease)
    }
}

private fun channel(key: DurableGuideChannelKey, name: String) = DurableGuideChannelRecord(
    key,
    listOf(name),
    artworkReference = "https://images.example.test/channel.jpg",
)

private fun programme(
    channel: DurableGuideChannelKey,
    winner: String,
    start: Long,
    end: Long?,
    title: String,
    description: String? = null,
) = DurableGuideProgrammeRecord(
    channelKey = channel,
    start = instant(start),
    end = end?.let(::instant),
    title = title,
    subtitle = "subtitle-$winner",
    description = description,
    categories = listOf("category"),
    episode = "S01E02",
    artworkReference = "https://images.example.test/programme.jpg",
)

private fun retention(anchor: Long, from: Long, until: Long) =
    DurableGuideRetention(instant(anchor), instant(from), instant(until))

private fun instant(milliseconds: Long): Instant = Instant.fromEpochMilliseconds(milliseconds)

private fun opaque(seed: String): String {
    val encoded = seed.map { it.code.toString(16).padStart(4, '0') }.joinToString("")
    return encoded.padEnd(DurableGuideLimits.OPAQUE_DIGEST_CHARS, '0')
        .take(DurableGuideLimits.OPAQUE_DIGEST_CHARS)
}

/** Small deterministic oracle. It exists only in commonTest, never as a production backend. */
internal class ContractInMemoryDurableGuideStore private constructor(
    private val state: SharedState,
) : DurableGuideStore {
    constructor() : this(SharedState())

    override val leaseIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_LEASE_IDLE_TIMEOUT_MILLIS
    override val generationIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_GENERATION_IDLE_TIMEOUT_MILLIS

    private val owner = Any()
    private val nextGeneration get() = state.nextGeneration
    private val revisions get() = state.revisions
    private val latest get() = state.latest
    private val generations get() = state.generations
    private val active get() = state.active
    private val entries get() = state.entries
    private val leases = mutableSetOf<Lease>()
    private val sourceMutations get() = state.sourceMutations
    private val guideMutations get() = state.guideMutations
    private var nowMillis: Long
        get() = state.nowMillis
        set(value) { state.nowMillis = value }

    fun reopen(): ContractInMemoryDurableGuideStore = ContractInMemoryDurableGuideStore(state)

    fun forceGenerationCountsForTest(
        generation: DurableGuideGeneration,
        channels: Long,
        programmes: Long,
    ) {
        val token = generation as GenerationToken
        require(token.owner === owner)
        token.syntheticChannels = channels
        token.syntheticProgrammes = programmes
    }

    fun forceGenerationWorkForTest(
        generation: DurableGuideGeneration,
        batches: Int,
        inputChannels: Long,
        inputProgrammes: Long,
    ) {
        val token = generation as GenerationToken
        require(token.owner === owner)
        token.batchCount = batches
        token.inputChannelRows = inputChannels
        token.inputProgrammeRows = inputProgrammes
    }

    fun advanceTimeBy(milliseconds: Long) {
        require(milliseconds >= 0)
        nowMillis += milliseconds
    }

    override suspend fun beginRefresh(
        key: DurableGuideKey,
        retention: DurableGuideRetention,
    ): DurableGuideGeneration {
        val value = (nextGeneration[key] ?: 0) + 1
        nextGeneration[key] = value
        latest[key]?.superseded = true
        bumpGuide(key)
        val token = GenerationToken(
            owner,
            key,
            value,
            retention,
            guideMutations.getValue(key),
            nowMillis + generationIdleTimeoutMillis,
        )
        latest[key] = token
        generations += token
        bumpSource(key.sourceKey)
        return token
    }

    override suspend fun renewGeneration(generation: DurableGuideGeneration): Boolean {
        val token = generation as? GenerationToken
        if (token == null || token.owner !== owner) throw DurableGuideStoreException.Stale()
        if (token.limitExceeded || !token.isLive(nowMillis) || latest[generation.key] !== token) return false
        token.expiresAt = nowMillis + generationIdleTimeoutMillis
        return true
    }

    override suspend fun abandon(generation: DurableGuideGeneration): Boolean {
        val token = generation as? GenerationToken
        if (token == null || token.owner !== owner) throw DurableGuideStoreException.Stale()
        if (token.abandoned || token.superseded || token.activated) return false
        token.abandoned = true
        if (latest[generation.key] === token) latest.remove(generation.key)
        bumpGuide(generation.key)
        bumpSource(generation.key.sourceKey)
        return true
    }

    override suspend fun stage(
        generation: DurableGuideGeneration,
        channels: List<DurableGuideChannelRecord>,
        programmes: List<DurableGuideProgrammeRecord>,
    ): DurableGuideCounts {
        val token = generation as? GenerationToken
        if (token == null || token.owner !== owner) throw DurableGuideStoreException.Stale()
        if (latest[generation.key] !== token || !token.isLive(nowMillis)) throw DurableGuideStoreException.Stale()
        token.batchCount += 1
        token.inputChannelRows += channels.size
        token.inputProgrammeRows += programmes.size
        if (
            token.batchCount > DurableGuideLimits.MAX_GENERATION_BATCHES ||
            token.inputChannelRows > DurableGuideLimits.MAX_INPUT_CHANNEL_ROWS ||
            token.inputProgrammeRows > DurableGuideLimits.MAX_INPUT_PROGRAMME_ROWS
        ) {
            token.limitExceeded = true
            throw DurableGuideStoreException.Limit()
        }
        if (channels.size + programmes.size > DurableGuideLimits.MAX_BATCH_ITEMS) {
            token.limitExceeded = true
            throw DurableGuideStoreException.Limit()
        }
        val stagedChannels = token.channels.toMutableMap()
        channels.forEach { candidate ->
            val current = stagedChannels[candidate.key]
            if (current == null || DurableGuideWinnerPolicy.compareChannels(candidate, current) < 0) {
                stagedChannels[candidate.key] = candidate
            }
        }
        val stagedProgrammes = token.programmes.toMutableMap()
        programmes.forEach { candidate ->
            require(candidate.start < token.retention.retainedUntil && candidate.effectiveEnd > token.retention.retainedFrom)
            val identity = ProgrammeIdentity(candidate.channelKey, candidate.start)
            val current = stagedProgrammes[identity]
            if (current == null || DurableGuideWinnerPolicy.compareProgrammes(candidate, current) < 0) {
                stagedProgrammes[identity] = candidate
            }
        }
        val channelCount = token.syntheticChannels + stagedChannels.size
        val programmeCount = token.syntheticProgrammes + stagedProgrammes.size
        if (
            channelCount > DurableGuideLimits.MAX_GENERATION_CHANNELS ||
            programmeCount > DurableGuideLimits.MAX_GENERATION_PROGRAMMES
        ) {
            token.limitExceeded = true
            throw DurableGuideStoreException.Limit()
        }
        token.channels.clear()
        token.channels.putAll(stagedChannels)
        token.programmes.clear()
        token.programmes.putAll(stagedProgrammes)
        token.expiresAt = nowMillis + generationIdleTimeoutMillis
        return DurableGuideCounts(channelCount, programmeCount)
    }

    override suspend fun activate(
        generation: DurableGuideGeneration,
        expected: DurableGuideCounts,
    ): DurableGuideActivation {
        val token = generation as? GenerationToken
        if (token == null || token.owner !== owner) throw DurableGuideStoreException.Stale()
        if (token.limitExceeded) throw DurableGuideStoreException.Limit()
        if (latest[generation.key] !== token) {
            return DurableGuideActivation.Superseded(active[generation.key]?.let(::snapshotOf))
        }
        if (!token.isLive(nowMillis)) throw DurableGuideStoreException.Stale()
        if (expected.channels == 0L && expected.programmes == 0L) throw DurableGuideStoreException.Limit()
        require(token.channels.size.toLong() == expected.channels && token.programmes.size.toLong() == expected.programmes)
        val revision = (revisions[generation.key] ?: 0) + 1
        revisions[generation.key] = revision
        val entry = Entry(
            key = generation.key,
            generation = token.value,
            revision = revision,
            mutation = token.mutation,
            retention = token.retention,
            channels = token.channels.values.sortedBy { it.key.value },
            programmes = token.programmes.values.sortedWith(compareBy({ it.channelKey.value }, { it.start }, { it.winnerKey.value })),
        )
        entries += entry
        active[generation.key] = entry
        latest.remove(generation.key)
        token.activated = true
        generations.remove(token)
        bumpSource(generation.key.sourceKey)
        return DurableGuideActivation.Published(snapshotOf(entry))
    }

    override suspend fun snapshot(key: DurableGuideKey): DurableGuideSnapshot? = active[key]?.let(::snapshotOf)

    override suspend fun snapshots(
        source: DurableGuideSourceSnapshot,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideSnapshotPage {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = source.token as? SourceToken
        if (token == null || token.owner !== owner || token.sourceKey != source.sourceKey) {
            throw DurableGuideStoreException.Stale()
        }
        val currentMutation = sourceMutations[source.sourceKey] ?: 0
        if (token.mutation != currentMutation) throw DurableGuideStoreException.Stale()
        val domain = SnapshotDomain(source.sourceKey, token.mutation)
        val offset = cursorOffset(after, domain)
        val values = active.values.filter { it.key.sourceKey == source.sourceKey }.sortedBy { it.key.feedId.value }
        val page = values.drop(offset).take(limit)
        return DurableGuideSnapshotPage(page.map(::snapshotOf), cursorOrNull(domain, offset + page.size, values.size))
    }

    override suspend fun acquire(snapshot: DurableGuideSnapshot): DurableGuideSnapshotLease? {
        val token = snapshot as? Snapshot ?: return null
        if (token.owner !== owner) return null
        val entry = entries.firstOrNull { it === token.entry && !it.cleanupStarted } ?: return null
        reapExpiredLeases()
        if (leases.size >= DurableGuideLimits.MAX_LIVE_LEASES) throw DurableGuideStoreException.Limit()
        val lease = Lease(owner, entry, nowMillis + leaseIdleTimeoutMillis)
        leases += lease
        return lease
    }

    override suspend fun renew(lease: DurableGuideSnapshotLease): Boolean =
        if (lease is Lease && lease.owner === owner && lease.valid && lease in leases && lease.expiresAt > nowMillis) {
            lease.expiresAt = nowMillis + leaseIdleTimeoutMillis
            true
        } else {
            if (lease is Lease && lease.owner === owner) {
                lease.valid = false
                leases.remove(lease)
            }
            false
        }

    override suspend fun release(lease: DurableGuideSnapshotLease) {
        if (lease is Lease && lease.owner === owner) {
            lease.valid = false
            leases.remove(lease)
        }
    }

    override suspend fun channels(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideChannelPage {
        val valid = validLease(lease)
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val domain = ChannelDomain(valid)
        val offset = cursorOffset(after, domain)
        val page = valid.entry.channels.drop(offset).take(limit)
        return DurableGuideChannelPage(page, cursorOrNull(domain, offset + page.size, valid.entry.channels.size))
    }

    override suspend fun programmeSearchRows(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideProgrammeSearchPage {
        val valid = validLease(lease)
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val domain = SearchDomain(valid)
        val offset = cursorOffset(after, domain)
        val page = valid.entry.programmes.drop(offset).take(limit).mapIndexed { index, programme ->
            DurableGuideProgrammeSearchRow(
                Locator(owner, valid.entry, offset + index),
                programme.start,
                programme.effectiveEnd,
                programme.title,
                programme.subtitle,
            )
        }
        return DurableGuideProgrammeSearchPage(
            page,
            cursorOrNull(domain, offset + page.size, valid.entry.programmes.size),
        )
    }

    override suspend fun programme(
        lease: DurableGuideSnapshotLease,
        locator: DurableGuideProgrammeLocator,
    ): DurableGuideProgrammeRecord? {
        val valid = lease as? Lease ?: return null
        val location = locator as? Locator ?: return null
        if (valid.owner !== owner || location.owner !== owner || valid.entry !== location.entry) {
            return null
        }
        if (!valid.isLive(nowMillis)) {
            return null
        }
        return valid.entry.programmes.getOrNull(location.ordinal)
    }

    override suspend fun window(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideWindowPage {
        require(from < until)
        require(limit in 1..DurableGuideLimits.MAX_WINDOW_ITEMS)
        val valid = validLease(lease)
        val domain = WindowDomain(valid, channelKey, from, until)
        val offset = cursorOffset(after, domain)
        val values = valid.entry.programmes.filter {
            it.channelKey == channelKey && it.start < until && it.effectiveEnd > from
        }
        val page = values.drop(offset).take(limit)
        val more = offset + page.size < values.size
        return DurableGuideWindowPage(page, cursorOrNull(domain, offset + page.size, values.size), more)
    }

    override suspend fun nowNext(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        at: Instant,
    ): DurableGuideNowNext {
        val programmes = validLease(lease).entry.programmes.filter { it.channelKey == channelKey }
        val current = programmes.lastOrNull { it.start <= at && it.effectiveEnd > at }
        val next = programmes.firstOrNull { it.start > at }
        return DurableGuideNowNext(current, next)
    }

    override suspend fun multiChannelWindow(
        lease: DurableGuideSnapshotLease,
        request: DurableGuideMultiChannelWindowRequest,
    ): DurableGuideMultiChannelWindow {
        val entry = validLease(lease).entry
        var remaining = request.totalLimit
        var payloadBytes = 0
        var payloadExhausted = false
        var anyTruncated = false
        val channels = request.channelKeys.map { channelKey ->
            val matching = entry.programmes.filter {
                it.channelKey == channelKey && it.start < request.until && it.effectiveEnd > request.from
            }
            val programmes = mutableListOf<DurableGuideProgrammeRecord>()
            for (programme in matching) {
                if (programmes.size >= request.perChannelLimit || remaining == 0 || payloadExhausted) break
                val bytes = DurableGuidePayloadSizing.programmeBytes(programme)
                if (payloadBytes + bytes > request.payloadByteLimit) {
                    payloadExhausted = true
                    break
                }
                programmes += programme
                payloadBytes += bytes
                remaining -= 1
            }
            val truncated = programmes.size < matching.size
            anyTruncated = anyTruncated || truncated
            DurableGuideChannelWindow(channelKey, programmes, truncated)
        }
        return DurableGuideMultiChannelWindow(channels, payloadBytes, anyTruncated)
    }

    override suspend fun prune(
        key: DurableGuideKey,
        expectedRevision: Long,
        expectedMutationEpoch: Long,
        retention: DurableGuideRetention,
    ): DurableGuidePruneResult {
        val current = active[key]
        if (
            current == null || current.revision != expectedRevision ||
            expectedMutationEpoch != (guideMutations[key] ?: 0)
        ) {
            return DurableGuidePruneResult.Superseded(current?.let(::snapshotOf))
        }
        if (
            retention.anchor != current.retention.anchor ||
            retention.retainedFrom < current.retention.retainedFrom ||
            retention.retainedUntil > current.retention.retainedUntil
        ) throw DurableGuideStoreException.Limit()
        val retainedProgrammes = current.programmes.filter {
            it.start < retention.retainedUntil && it.effectiveEnd > retention.retainedFrom
        }
        generations.filter { it.key == key }.forEach { it.superseded = true }
        latest.remove(key)
        bumpGuide(key)
        bumpSource(key.sourceKey)
        if (retainedProgrammes.size == current.programmes.size) {
            return DurableGuidePruneResult.Unchanged(snapshotOf(current))
        }
        val revision = (revisions[key] ?: 0) + 1
        revisions[key] = revision
        val generation = (nextGeneration[key] ?: current.generation) + 1
        nextGeneration[key] = generation
        val entry = Entry(
            key,
            generation,
            revision,
            guideMutations.getValue(key),
            retention,
            current.channels,
            retainedProgrammes,
        )
        entries += entry
        active[key] = entry
        return DurableGuidePruneResult.Published(snapshotOf(entry))
    }

    override suspend fun deleteGuide(
        key: DurableGuideKey,
        expectedRevision: Long?,
        expectedMutationEpoch: Long?,
    ): DurableGuideDeleteResult {
        if ((expectedRevision == null) != (expectedMutationEpoch == null)) {
            throw DurableGuideStoreException.Stale()
        }
        val current = active[key]
        if (
            expectedRevision != null &&
            (current?.revision != expectedRevision || expectedMutationEpoch != (guideMutations[key] ?: 0))
        ) {
            return DurableGuideDeleteResult.Superseded(current?.let(::snapshotOf))
        }
        generations.filter { it.key == key }.forEach { it.abandoned = true }
        val revision = (revisions[key] ?: 0) + 1
        bumpGuide(key)
        revisions[key] = revision
        active.remove(key)
        latest.remove(key)
        bumpSource(key.sourceKey)
        return DurableGuideDeleteResult.Deleted(revision)
    }

    override suspend fun sourceSnapshot(sourceKey: DurableGuideSourceKey): DurableGuideSourceSnapshot =
        DurableGuideSourceSnapshot(
            sourceKey = sourceKey,
            feedCount = active.keys.count { it.sourceKey == sourceKey },
            token = SourceToken(owner, sourceKey, sourceMutations[sourceKey] ?: 0),
        )

    override suspend fun deleteSource(
        sourceKey: DurableGuideSourceKey,
        expected: DurableGuideSourceToken?,
    ): DurableGuideSourceDeleteResult {
        val sourceEntries = active.filterKeys { it.sourceKey == sourceKey }
        val stagedKeys = latest.keys.filter { it.sourceKey == sourceKey }.toSet()
        val stagedOnlyKeys = stagedKeys - sourceEntries.keys
        if (expected != null) {
            val token = expected as? SourceToken
            if (
                token == null || token.owner !== owner || token.sourceKey != sourceKey ||
                token.mutation != (sourceMutations[sourceKey] ?: 0)
            ) {
                return DurableGuideSourceDeleteResult.Superseded(sourceEntries.size, stagedOnlyKeys.size)
            }
        }
        val sourceKeys = (sourceEntries.keys + stagedKeys).toSet()
        sourceKeys.forEach { key ->
            revisions[key] = (revisions[key] ?: 0) + 1
            bumpGuide(key)
            active.remove(key)
            latest.remove(key)?.abandoned = true
        }
        bumpSource(sourceKey)
        return DurableGuideSourceDeleteResult.Deleted(sourceEntries.size, stagedOnlyKeys.size)
    }

    override suspend fun cleanupUnreachable(maxRows: Int): DurableGuideCleanupResult {
        if (maxRows !in 1..DurableGuideLimits.MAX_CLEANUP_ROWS) throw DurableGuideStoreException.Limit()
        reapExpiredLeases()
        var remaining = maxRows
        var removed = 0
        val expiredWriter = generations.firstOrNull { generation ->
            latest[generation.key] === generation && !generation.abandoned &&
                !generation.superseded && !generation.activated && !generation.limitExceeded &&
                generation.expiresAt <= nowMillis
        }
        if (expiredWriter != null) {
            expiredWriter.limitExceeded = true
            latest.remove(expiredWriter.key)
            remaining -= 1
        }
        val generationIterator = generations.iterator()
        while (generationIterator.hasNext() && remaining > 0) {
            val generation = generationIterator.next()
            if (generation.isLive(nowMillis) && latest[generation.key] === generation) continue
            val rows = generation.unreachableRows
                ?: (generation.channels.size + generation.programmes.size).also { generation.unreachableRows = it }
            val take = min(rows, remaining)
            generation.unreachableRows = rows - take
            removed += take
            remaining -= take
            if (generation.unreachableRows == 0) {
                if (latest[generation.key] === generation) latest.remove(generation.key)
                generationIterator.remove()
            }
        }
        val iterator = entries.iterator()
        while (iterator.hasNext() && remaining > 0) {
            val entry = iterator.next()
            if (active[entry.key] === entry || leases.any { it.isLive(nowMillis) && it.entry === entry }) continue
            val take = min(entry.unreachableRows, remaining)
            if (take > 0) entry.cleanupStarted = true
            entry.unreachableRows -= take
            removed += take
            remaining -= take
            if (entry.unreachableRows == 0) iterator.remove()
        }
        val hasMore = generations.any { !it.isLive(nowMillis) || latest[it.key] !== it } || entries.any {
            active[it.key] !== it && leases.none { lease -> lease.isLive(nowMillis) && lease.entry === it }
        }
        return DurableGuideCleanupResult(removed, hasMore)
    }

    private fun validLease(value: DurableGuideSnapshotLease): Lease {
        val lease = value as? Lease
        if (lease == null || lease.owner !== owner || !lease.isLive(nowMillis) || lease !in leases) {
            throw DurableGuideStoreException.Stale()
        }
        return lease
    }

    private fun cursorOffset(value: DurableGuideCursor?, domain: CursorDomain): Int {
        if (value == null) return 0
        val cursor = value as? Cursor
        if (cursor == null || cursor.owner !== owner || cursor.domain != domain) {
            throw DurableGuideStoreException.Stale()
        }
        return cursor.offset
    }

    private fun reapExpiredLeases() {
        leases.removeAll { !it.isLive(nowMillis) }
    }

    private fun cursorOrNull(domain: CursorDomain, offset: Int, size: Int): DurableGuideCursor? =
        if (offset < size) Cursor(owner, domain, offset) else null

    private fun bumpSource(sourceKey: DurableGuideSourceKey) {
        sourceMutations[sourceKey] = (sourceMutations[sourceKey] ?: 0) + 1
    }

    private fun bumpGuide(key: DurableGuideKey) {
        guideMutations[key] = (guideMutations[key] ?: 0) + 1
    }

    private fun snapshotOf(entry: Entry): DurableGuideSnapshot =
        Snapshot(owner, entry, guideMutations[entry.key] ?: entry.mutation)

    private class GenerationToken(
        val owner: Any,
        override val key: DurableGuideKey,
        val value: Long,
        val retention: DurableGuideRetention,
        val mutation: Long,
        var expiresAt: Long,
        val channels: MutableMap<DurableGuideChannelKey, DurableGuideChannelRecord> = mutableMapOf(),
        val programmes: MutableMap<ProgrammeIdentity, DurableGuideProgrammeRecord> = mutableMapOf(),
    ) : DurableGuideGeneration {
        var abandoned: Boolean = false
        var superseded: Boolean = false
        var activated: Boolean = false
        var unreachableRows: Int? = null
        var syntheticChannels: Long = 0
        var syntheticProgrammes: Long = 0
        var limitExceeded: Boolean = false
        var batchCount: Int = 0
        var inputChannelRows: Long = 0
        var inputProgrammeRows: Long = 0

        fun isLive(nowMillis: Long): Boolean =
            !abandoned && !superseded && !activated && !limitExceeded && expiresAt > nowMillis

        override fun toString(): String = "DurableGuideGeneration(key=<redacted>, value=$value)"
    }

    private data class ProgrammeIdentity(val channel: DurableGuideChannelKey, val start: Instant)

    private class Entry(
        val key: DurableGuideKey,
        val generation: Long,
        val revision: Long,
        val mutation: Long,
        val retention: DurableGuideRetention,
        val channels: List<DurableGuideChannelRecord>,
        val programmes: List<DurableGuideProgrammeRecord>,
    ) {
        var unreachableRows: Int = channels.size + programmes.size
        var cleanupStarted: Boolean = false

    }

    private class Snapshot(
        val owner: Any,
        val entry: Entry,
        override val mutationEpoch: Long,
    ) : DurableGuideSnapshot {
        override val key: DurableGuideKey get() = entry.key
        override val generation: Long get() = entry.generation
        override val revision: Long get() = entry.revision
        override val counts: DurableGuideCounts
            get() = DurableGuideCounts(entry.channels.size.toLong(), entry.programmes.size.toLong())
        override val retention: DurableGuideRetention get() = entry.retention
        override fun toString(): String =
            "DurableGuideSnapshot(key=<redacted>, generation=$generation, revision=$revision, " +
                "channels=${counts.channels}, programmes=${counts.programmes}, retention=$retention)"
    }

    private class SourceToken(
        val owner: Any,
        val sourceKey: DurableGuideSourceKey,
        val mutation: Long,
    ) : DurableGuideSourceToken {
        override fun toString(): String = "DurableGuideSourceToken(<redacted>)"
    }

    private sealed interface CursorDomain
    private data class SnapshotDomain(
        val sourceKey: DurableGuideSourceKey,
        val mutation: Long,
    ) : CursorDomain
    private data class ChannelDomain(val lease: Lease) : CursorDomain
    private data class SearchDomain(val lease: Lease) : CursorDomain
    private data class WindowDomain(
        val lease: Lease,
        val channelKey: DurableGuideChannelKey,
        val from: Instant,
        val until: Instant,
    ) : CursorDomain

    private class Lease(
        val owner: Any,
        val entry: Entry,
        var expiresAt: Long,
    ) : DurableGuideSnapshotLease {
        var valid: Boolean = true
        fun isLive(nowMillis: Long): Boolean {
            if (expiresAt <= nowMillis) valid = false
            return valid
        }
        override fun toString(): String = "DurableGuideSnapshotLease(<redacted>)"
    }

    private class Cursor(val owner: Any, val domain: CursorDomain, val offset: Int) : DurableGuideCursor {
        override fun toString(): String = "DurableGuideCursor(<redacted>)"
    }

    private class Locator(val owner: Any, val entry: Entry, val ordinal: Int) : DurableGuideProgrammeLocator {
        override fun toString(): String = "DurableGuideProgrammeLocator(<redacted>)"
    }

    private class SharedState {
        val nextGeneration = mutableMapOf<DurableGuideKey, Long>()
        val revisions = mutableMapOf<DurableGuideKey, Long>()
        val latest = mutableMapOf<DurableGuideKey, GenerationToken>()
        val generations = mutableListOf<GenerationToken>()
        val active = mutableMapOf<DurableGuideKey, Entry>()
        val entries = mutableListOf<Entry>()
        val sourceMutations = mutableMapOf<DurableGuideSourceKey, Long>()
        val guideMutations = mutableMapOf<DurableGuideKey, Long>()
        var nowMillis: Long = 0
    }
}
