package com.getair.core.catalog

import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.CategoryId
import com.getair.iptv.model.Catchup
import com.getair.iptv.model.CatchupType
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.IptvPlaylistEntry
import com.getair.iptv.model.IptvPlaylistEntryMetadata
import com.getair.iptv.model.PlaylistEntryId
import com.getair.iptv.model.PlaylistEntryKind
import com.getair.iptv.model.toCatalogMetadata
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs

/** Runs unchanged against SQLDelight and real IndexedDB catalog stores. */
internal suspend fun verifyDurableM3uCatalogContract(
    open: suspend () -> DurableCatalogStore,
) {
    val source = LocalSourceId("m3u-contract-source")
    val catalog = DurableCatalogKey(DurableCatalogKind.M3uPlaylistEntry, "primary")
    val streamSecret = "stream-secret-must-not-persist"
    val headerSecret = "header-secret-must-not-persist"
    val catchupSecret = "catchup-secret-must-not-persist"
    val ephemeral = IptvPlaylistEntry(
        id = PlaylistEntryId("entry-1"),
        name = "World News",
        streamUrl = "https://media.example.test/live.m3u8?token=$streamSecret",
        kind = PlaylistEntryKind.Live,
        durationSeconds = 0.0,
        categoryIds = listOf(CategoryId("news")),
        epgChannelId = EpgChannelId("world.news"),
        logoUrl = "https://images.example.test/world-news.png",
        headers = mapOf("Authorization" to "Bearer $headerSecret"),
        catchup = Catchup(
            type = CatchupType.Append,
            source = "https://archive.example.test/{start}?token=$catchupSecret",
            days = 7.0,
        ),
        attributes = mapOf("provider-private" to streamSecret),
    )
    val metadata = ephemeral.toCatalogMetadata()

    // The same serializer used by both backends has no playback-bearing fields.
    val encodedMetadata = Json.encodeToString(IptvPlaylistEntryMetadata.serializer(), metadata)
    listOf("streamUrl", "headers", "attributes", "\"source\"").forEach { forbiddenField ->
        assertFalse(forbiddenField in encodedMetadata)
    }
    listOf(streamSecret, headerSecret, catchupSecret).forEach { secret ->
        assertFalse(secret in encodedMetadata)
        assertFalse(secret in metadata.toString())
    }

    var store = open()
    val generation = store.beginRefresh(source)
    store.stageCatalogBatch(
        generation,
        catalog,
        listOf(DurableCatalogItem.M3uPlaylistEntry(metadata)),
    )
    store.activate(generation, CatalogGenerationCounts(1, 0, 0))
    store.close()

    store = open()
    val restored = assertIs<DurableCatalogItem.M3uPlaylistEntry>(
        store.catalogPage(source, catalog, limit = 1).items.single().item,
    ).value
    assertEquals(metadata, restored)
    listOf(streamSecret, headerSecret, catchupSecret).forEach { secret ->
        assertFalse(secret in restored.toString())
    }

    val unsafeGeneration = store.beginRefresh(source)
    val unsafeSecret = "unsafe-artwork-secret"
    val failure = assertFailsWith<IllegalArgumentException> {
        store.stageCatalogBatch(
            unsafeGeneration,
            catalog,
            listOf(
                DurableCatalogItem.M3uPlaylistEntry(
                    metadata.copy(logoUrl = "https://images.example.test/logo.png?token=$unsafeSecret"),
                ),
            ),
        )
    }
    assertFalse(unsafeSecret in failure.message.orEmpty())
    assertEquals(
        metadata,
        assertIs<DurableCatalogItem.M3uPlaylistEntry>(
            store.catalogPage(source, catalog, limit = 1).items.single().item,
        ).value,
    )
    store.close()
}
