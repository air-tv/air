package com.getair.core.catalog

import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgProgramme
import com.getair.stremio.model.MetaLink
import com.getair.stremio.model.MetaPreview
import kotlinx.datetime.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

internal suspend fun verifyIndexedDbCatalogContract(
    open: suspend (String, CatalogStorageOptions) -> DurableCatalogStore,
    databaseName: String,
) {
    val source = LocalSourceId("browser-source")
    val otherSource = LocalSourceId("browser-other")
    val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "popular")
    var store = open(databaseName, CatalogStorageOptions(startupCleanupRows = 1))

    val first = store.beginRefresh(source)
    store.stageCatalogBatch(first, catalog, browserMetas("old", 3))
    store.stageGuideBatch(
        first,
        channels = listOf(
            EpgChannel(EpgChannelId("one"), listOf("One")),
            EpgChannel(EpgChannelId("two"), listOf("Two")),
        ),
        programmes = listOf(
            browserProgramme("one-a", "one", 1_000, 2_000),
            browserProgramme("one-b", "one", 2_000, 3_000),
            browserProgramme("two-a", "two", 1_000, 3_000),
        ),
    )
    val firstStatus = store.activate(first, CatalogGenerationCounts(3, 2, 3))
    assertEquals(1, firstStatus.revision)

    val other = store.beginRefresh(otherSource)
    store.stageCatalogBatch(other, catalog, browserMetas("other", 1))
    store.activate(other, CatalogGenerationCounts(1, 0, 0))

    val second = store.beginRefresh(source)
    store.stageCatalogBatch(second, catalog, browserMetas("new", 5))
    assertFailsWith<CatalogStoreException> {
        store.activate(second, CatalogGenerationCounts(4, 0, 0))
    }
    assertEquals(
        listOf("old-0", "old-1", "old-2"),
        store.catalogPage(source, catalog, limit = 10).items.map(::browserItemId),
    )
    store.activate(second, CatalogGenerationCounts(5, 0, 0))

    val firstPage = store.catalogPage(source, catalog, limit = 2)
    val secondPage = store.catalogPage(source, catalog, firstPage.nextCursor, limit = 2)
    val thirdPage = store.catalogPage(source, catalog, secondPage.nextCursor, limit = 2)
    assertEquals(
        listOf("new-0", "new-1", "new-2", "new-3", "new-4"),
        (firstPage.items + secondPage.items + thirdPage.items).map(::browserItemId),
    )
    assertNull(thirdPage.nextCursor)
    assertEquals(
        listOf("other-0"),
        store.catalogPage(otherSource, catalog, limit = 10).items.map(::browserItemId),
    )

    // The old guide generation is unreachable immediately after activation.
    assertTrue(
        store.guideWindow(
            source,
            EpgChannelId("one"),
            browserInstant(1_500),
            browserInstant(2_500),
            10,
        ).isEmpty(),
    )

    val guideGeneration = store.beginRefresh(source)
    store.stageGuideBatch(
        guideGeneration,
        channels = listOf(
            EpgChannel(EpgChannelId("one"), listOf("One")),
            EpgChannel(EpgChannelId("two"), listOf("Two")),
        ),
        programmes = listOf(
            browserProgramme("one-a", "one", 1_000, 2_000),
            browserProgramme("one-b", "one", 2_000, 3_000),
            browserProgramme("two-a", "two", 1_000, 3_000),
        ),
    )
    store.activate(guideGeneration, CatalogGenerationCounts(0, 2, 3))
    val channels = store.channelPage(source, limit = 1)
    assertEquals("One", channels.items.single().channel.displayNames.single())
    assertEquals(
        "Two",
        store.channelPage(source, channels.nextCursor, 1).items.single().channel.displayNames.single(),
    )
    assertEquals(
        listOf("one-a", "one-b"),
        store.guideWindow(
            source,
            EpgChannelId("one"),
            browserInstant(1_500),
            browserInstant(2_500),
            10,
        ).map(DurableGuideProgramme::providerEventId),
    )
    val nowNext = store.nowNext(source, EpgChannelId("one"), browserInstant(1_500))
    assertEquals("one-a", nowNext.current?.title)
    assertEquals("one-b", nowNext.next?.title)

    val stale = store.beginRefresh(source)
    store.stageCatalogBatch(stale, catalog, browserMetas("stale", 1))
    store.deleteSource(source)
    assertNull(store.sourceStatus(source))
    assertTrue(store.channelPage(source, limit = 10).items.isEmpty())
    assertFailsWith<CatalogStoreException> {
        store.activate(stale, CatalogGenerationCounts(1, 0, 0))
    }

    val superseded = store.beginRefresh(source)
    val latest = store.beginRefresh(source)
    assertFailsWith<CatalogStoreException> {
        store.stageCatalogBatch(superseded, catalog, browserMetas("superseded", 1))
    }
    store.stageCatalogBatch(latest, catalog, browserMetas("latest", 1))
    store.activate(latest, CatalogGenerationCounts(1, 0, 0))

    // Replacing one provider identity must not inflate the exact activation count.
    val replacement = store.beginRefresh(source)
    store.stageCatalogBatch(replacement, catalog, browserMetas("same", 1))
    store.stageCatalogBatch(replacement, catalog, browserMetas("same", 1))
    store.activate(replacement, CatalogGenerationCounts(1, 0, 0))
    assertEquals(listOf("same-0"), store.catalogPage(source, catalog, limit = 10).items.map(::browserItemId))

    val orphan = store.beginRefresh(source)
    store.stageCatalogBatch(orphan, catalog, browserMetas("orphan", 3))
    store.close()

    // Opening performs one bounded cleanup pass; the active generation remains readable.
    store = open(databaseName, CatalogStorageOptions(startupCleanupRows = 1))
    assertEquals(listOf("same-0"), store.catalogPage(source, catalog, limit = 10).items.map(::browserItemId))
    var passes = 0
    var cleanup = store.cleanupUnreachable(2)
    while (cleanup.hasMore && passes < 100) {
        assertTrue(cleanup.removedRows <= 2)
        cleanup = store.cleanupUnreachable(2)
        passes += 1
    }
    assertFalse(cleanup.hasMore)
    assertTrue(passes > 0)
    store.close()
}

internal suspend fun verifyIndexedDbRejectsUnsafeMetadata(
    open: suspend (String, CatalogStorageOptions) -> DurableCatalogStore,
    databaseName: String,
) {
    val store = open(databaseName, CatalogStorageOptions())
    val source = LocalSourceId("unsafe")
    val generation = store.beginRefresh(source)
    val secret = "never-persist-this"
    val failure = assertFailsWith<IllegalArgumentException> {
        store.stageCatalogBatch(
            generation,
            DurableCatalogKey(DurableCatalogKind.Stremio, "unsafe"),
            listOf(
                DurableCatalogItem.Stremio(
                    MetaPreview(
                        id = "unsafe",
                        type = "movie",
                        name = "Unsafe",
                        poster = "https://images.example.test/poster.jpg?token=$secret",
                    ),
                ),
            ),
        )
    }
    assertFalse(failure.message.orEmpty().contains(secret))
    store.close()
}

internal suspend fun verifyIndexedDbBoundedLargeFixture(
    open: suspend (String, CatalogStorageOptions) -> DurableCatalogStore,
    databaseName: String,
) {
    val store = open(databaseName, CatalogStorageOptions())
    val source = LocalSourceId("large-source")
    val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "large")
    val generation = store.beginRefresh(source)
    store.stageCatalogBatch(generation, catalog, browserMetas("first", MAX_BATCH_ITEMS))
    store.stageCatalogBatch(generation, catalog, browserMetas("second", MAX_BATCH_ITEMS))
    store.activate(generation, CatalogGenerationCounts(512, 0, 0))

    val identities = mutableSetOf<String>()
    var cursor: Long? = null
    do {
        val page = store.catalogPage(source, catalog, cursor, limit = 37)
        assertTrue(page.items.size <= 37)
        identities += page.items.map(::browserItemId)
        cursor = page.nextCursor
    } while (cursor != null)
    assertEquals(512, identities.size)

    repeat(16) { index ->
        val isolated = LocalSourceId("isolated-$index")
        val isolatedGeneration = store.beginRefresh(isolated)
        store.stageCatalogBatch(isolatedGeneration, catalog, browserMetas("isolated-$index", 1))
        store.activate(isolatedGeneration, CatalogGenerationCounts(1, 0, 0))
    }
    store.deleteSource(LocalSourceId("isolated-7"))
    assertTrue(store.catalogPage(LocalSourceId("isolated-7"), catalog, limit = 1).items.isEmpty())
    assertEquals(
        "isolated-8-0",
        browserItemId(store.catalogPage(LocalSourceId("isolated-8"), catalog, limit = 1).items.single()),
    )
    assertEquals(512, identities.size)
    store.close()
}

private fun browserMetas(prefix: String, count: Int): List<DurableCatalogItem> =
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

private fun browserItemId(item: DurableCatalogPageItem): String =
    (item.item as DurableCatalogItem.Stremio).value.id

private fun browserProgramme(
    eventId: String,
    channelId: String,
    startMs: Long,
    endMs: Long,
) = DurableGuideProgramme(
    eventId,
    EpgProgramme(
        channelId = EpgChannelId(channelId),
        start = browserInstant(startMs),
        end = browserInstant(endMs),
        title = eventId,
    ),
)

private fun browserInstant(milliseconds: Long): Instant = Instant.fromEpochMilliseconds(milliseconds)
