package com.getair.core.catalog

import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgNowNext
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.IptvChannelMetadata
import com.getair.iptv.model.IptvEpisodeMetadata
import com.getair.iptv.model.IptvMovieMetadata
import com.getair.iptv.model.IptvSeriesMetadata
import com.getair.stremio.model.MetaPreview
import kotlinx.datetime.Instant

/** An unreachable source generation being populated by a refresh. */
class CatalogGeneration internal constructor(
    internal val sourceId: LocalSourceId,
    internal val value: Long,
) {
    override fun toString(): String = "CatalogGeneration(source=<redacted>, value=$value)"
}

enum class DurableCatalogKind(internal val storageValue: String) {
    Stremio("stremio"),
    IptvChannel("iptv-channel"),
    IptvMovie("iptv-movie"),
    IptvSeries("iptv-series"),
    IptvEpisode("iptv-episode"),
}

data class DurableCatalogKey(
    val kind: DurableCatalogKind,
    val catalogId: String,
) {
    init {
        require(catalogId.isNotBlank() && catalogId.length <= MAX_CATALOG_ID_CHARS) {
            "Catalog ID must be between 1 and $MAX_CATALOG_ID_CHARS characters"
        }
        require('\u0000' !in catalogId) { "Catalog ID contains an invalid character" }
    }
}

sealed interface DurableCatalogItem {
    data class Stremio(val value: MetaPreview) : DurableCatalogItem
    data class IptvChannel(val value: IptvChannelMetadata) : DurableCatalogItem
    data class IptvMovie(val value: IptvMovieMetadata) : DurableCatalogItem
    data class IptvSeries(val value: IptvSeriesMetadata) : DurableCatalogItem
    data class IptvEpisode(val value: IptvEpisodeMetadata) : DurableCatalogItem
}

data class DurableCatalogPageItem(
    val sortOrder: Long,
    val item: DurableCatalogItem,
)

data class DurableCatalogPage(
    val items: List<DurableCatalogPageItem>,
    val nextCursor: Long?,
)

data class DurableChannelPageItem(
    val sortOrder: Long,
    val channel: EpgChannel,
)

data class DurableChannelPage(
    val items: List<DurableChannelPageItem>,
    val nextCursor: Long?,
)

data class DurableGuideProgramme(
    val providerEventId: String,
    val programme: EpgProgramme,
) {
    init {
        require(providerEventId.isNotBlank() && providerEventId.length <= MAX_EVENT_ID_CHARS) {
            "Provider event ID must be between 1 and $MAX_EVENT_ID_CHARS characters"
        }
        require('\u0000' !in providerEventId) { "Provider event ID contains an invalid character" }
    }
}

data class CatalogGenerationCounts(
    val catalogItems: Long,
    val channels: Long,
    val programmes: Long,
) {
    init {
        require(catalogItems >= 0 && channels >= 0 && programmes >= 0)
    }
}

data class CatalogSourceStatus(
    val activeGeneration: Long?,
    val revision: Long,
    val activatedAt: Instant?,
)

data class CatalogCleanupResult(
    val removedRows: Int,
    val hasMore: Boolean,
)

data class CatalogStorageOptions(
    val startupCleanupRows: Int = DEFAULT_CLEANUP_ROWS,
) {
    init { require(startupCleanupRows in 1..10_000) }
}

/**
 * Durable, rebuildable media metadata. All reads are active-generation only.
 *
 * IPTV values use the protocol library's dedicated metadata contracts. Legacy
 * playback-bearing channel/movie/episode models have no storage variant here.
 */
interface DurableCatalogStore {
    suspend fun beginRefresh(sourceId: LocalSourceId): CatalogGeneration

    /** Appends one bounded, single-catalog batch to an unreachable generation. */
    suspend fun stageCatalogBatch(
        generation: CatalogGeneration,
        catalog: DurableCatalogKey,
        items: List<DurableCatalogItem>,
    )

    suspend fun stageGuideBatch(
        generation: CatalogGeneration,
        channels: List<EpgChannel> = emptyList(),
        programmes: List<DurableGuideProgramme> = emptyList(),
    )

    /** Atomically publishes only if all staged counts exactly match [expected]. */
    suspend fun activate(
        generation: CatalogGeneration,
        expected: CatalogGenerationCounts,
    ): CatalogSourceStatus

    suspend fun sourceStatus(sourceId: LocalSourceId): CatalogSourceStatus?

    suspend fun catalogPage(
        sourceId: LocalSourceId,
        catalog: DurableCatalogKey,
        afterSortOrder: Long? = null,
        limit: Int,
    ): DurableCatalogPage

    suspend fun channelPage(
        sourceId: LocalSourceId,
        afterSortOrder: Long? = null,
        limit: Int,
    ): DurableChannelPage

    suspend fun guideWindow(
        sourceId: LocalSourceId,
        channelId: EpgChannelId,
        from: Instant,
        until: Instant,
        limit: Int,
    ): List<DurableGuideProgramme>

    suspend fun nowNext(
        sourceId: LocalSourceId,
        channelId: EpgChannelId,
        at: Instant,
    ): EpgNowNext

    /** Makes a source unreadable immediately; physical rows are pruned in chunks. */
    suspend fun deleteSource(sourceId: LocalSourceId)

    /** Removes at most [maxRows] from inactive, failed, or deleted generations. */
    suspend fun cleanupUnreachable(maxRows: Int = DEFAULT_CLEANUP_ROWS): CatalogCleanupResult

    fun close()
}

class CatalogStoreException internal constructor(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

internal const val MAX_CATALOG_ID_CHARS = 256
internal const val MAX_EVENT_ID_CHARS = 256
internal const val MAX_BATCH_ITEMS = 256
internal const val MAX_CATALOG_PAGE_ITEMS = 200
internal const val MAX_CHANNEL_PAGE_ITEMS = 200
internal const val MAX_GUIDE_WINDOW_ITEMS = 1_000
internal const val MAX_GUIDE_WINDOW_MILLIS = 14L * 24 * 60 * 60 * 1_000
internal const val MAX_PAYLOAD_BYTES = 256 * 1_024
internal const val DEFAULT_CLEANUP_ROWS = 512
