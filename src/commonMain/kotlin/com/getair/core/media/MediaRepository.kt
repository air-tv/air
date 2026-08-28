package com.getair.core.media

import com.getair.core.household.HouseholdProfileId
import com.getair.core.source.LocalSourceId
import com.getair.core.source.LocalSourceState
import com.getair.iptv.model.IptvChannel
import com.getair.iptv.model.IptvMovie
import com.getair.iptv.model.IptvSeries
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Media discovered from one configured source.
 *
 * These are the protocol libraries' models rather than application copies. In
 * particular, IPTV playback fields remain exactly where the IPTV protocol model
 * requires them; source credentials never belong in this index.
 */
data class SourceMedia(
    val stremioCatalogs: Map<String, List<MetaPreview>> = emptyMap(),
    val liveChannels: List<IptvChannel> = emptyList(),
    val iptvMovies: List<IptvMovie> = emptyList(),
    val iptvSeries: List<IptvSeries> = emptyList(),
)

data class MediaSnapshot(
    val sources: Map<LocalSourceId, SourceMedia> = emptyMap(),
    val revision: Long = 0,
) {
    init { require(revision >= 0) }

    fun source(id: LocalSourceId): SourceMedia? = sources[id]

    /**
     * Returns enabled media in the exact source order visible to [profileId].
     * Source grouping is retained so duplicate provider IDs can never erase or
     * accidentally inherit another source's ownership.
     */
    fun visibleTo(
        profileId: HouseholdProfileId,
        sourceState: LocalSourceState,
    ): ProfileMediaSnapshot = ProfileMediaSnapshot(
        sources = sourceState.sourcesFor(profileId).mapNotNull { source ->
            sources[source.id]?.let { media -> SourceMediaEntry(source.id, media) }
        },
        mediaRevision = revision,
        sourceRevision = sourceState.revision,
    )
}

data class SourceMediaEntry(
    val sourceId: LocalSourceId,
    val media: SourceMedia,
)

/** A deterministic, source-grouped read view for one household profile. */
data class ProfileMediaSnapshot(
    val sources: List<SourceMediaEntry>,
    val mediaRevision: Long,
    val sourceRevision: Long,
) {
    fun mergedLiveChannels(): List<IptvChannel> = sources.flatMap { it.media.liveChannels }
    fun mergedIptvMovies(): List<IptvMovie> = sources.flatMap { it.media.iptvMovies }
    fun mergedIptvSeries(): List<IptvSeries> = sources.flatMap { it.media.iptvSeries }

    /** Merges one catalog without conflating catalogs that merely share an item ID. */
    fun mergedStremioCatalog(catalogId: String): List<MetaPreview> =
        sources.flatMap { it.media.stremioCatalogs[catalogId].orEmpty() }
}

interface LocalMediaStore {
    val snapshot: StateFlow<MediaSnapshot>
    suspend fun replace(snapshot: MediaSnapshot)
}

class InMemoryMediaStore(initial: MediaSnapshot = MediaSnapshot()) : LocalMediaStore {
    private val mutableSnapshot = MutableStateFlow(initial.detached())
    override val snapshot: StateFlow<MediaSnapshot> = mutableSnapshot.asStateFlow()

    override suspend fun replace(snapshot: MediaSnapshot) {
        mutableSnapshot.value = snapshot
    }
}

fun interface MediaSyncSource {
    suspend fun load(previous: MediaSnapshot): MediaSnapshot
}

sealed interface MediaRefreshResult {
    data object LocalOnly : MediaRefreshResult
    data class Unchanged(val revision: Long) : MediaRefreshResult
    data class Updated(val revision: Long) : MediaRefreshResult
}

class LocalFirstMediaRepository(
    private val store: LocalMediaStore,
    private val syncSource: MediaSyncSource? = null,
) {
    private val commands = Mutex()
    val snapshot: StateFlow<MediaSnapshot> = store.snapshot

    /**
     * Atomically replaces one source while retaining every unrelated source
     * value. Repeating an equivalent replacement is a stable no-op.
     */
    suspend fun replaceSource(sourceId: LocalSourceId, media: SourceMedia) = commands.withLock {
        val previous = snapshot.value
        val detached = media.detached()
        if (previous.sources[sourceId] == detached) return@withLock
        store.replace(
            previous.copy(
                sources = previous.sources + (sourceId to detached),
                revision = previous.nextRevision(),
            ),
        )
    }

    /** Removing an absent source is intentionally idempotent. */
    suspend fun removeSource(sourceId: LocalSourceId) = commands.withLock {
        val previous = snapshot.value
        if (sourceId !in previous.sources) return@withLock
        store.replace(
            previous.copy(
                sources = previous.sources - sourceId,
                revision = previous.nextRevision(),
            ),
        )
    }

    /** Full local restore/import seam. Revision remains repository-owned. */
    suspend fun replaceLocal(snapshot: MediaSnapshot) = commands.withLock {
        replaceAllIfChanged(snapshot.sources)
    }

    suspend fun refresh(): MediaRefreshResult {
        val source = syncSource ?: return MediaRefreshResult.LocalOnly
        return commands.withLock {
            val next = source.load(snapshot.value)
            if (!replaceAllIfChanged(next.sources)) {
                MediaRefreshResult.Unchanged(snapshot.value.revision)
            } else {
                MediaRefreshResult.Updated(snapshot.value.revision)
            }
        }
    }

    private suspend fun replaceAllIfChanged(sources: Map<LocalSourceId, SourceMedia>): Boolean {
        val previous = snapshot.value
        val detached = sources.detached()
        if (previous.sources == detached) return false
        store.replace(previous.copy(sources = detached, revision = previous.nextRevision()))
        return true
    }
}

private fun MediaSnapshot.nextRevision(): Long {
    check(revision < Long.MAX_VALUE) { "Media revision is exhausted" }
    return revision + 1
}

private fun MediaSnapshot.detached(): MediaSnapshot = copy(sources = sources.detached())

private fun Map<LocalSourceId, SourceMedia>.detached(): Map<LocalSourceId, SourceMedia> =
    entries.associate { (sourceId, media) -> sourceId to media.detached() }

private fun SourceMedia.detached(): SourceMedia = SourceMedia(
    stremioCatalogs = stremioCatalogs.entries
        .sortedBy(Map.Entry<String, List<MetaPreview>>::key)
        .associate { (catalogId, items) -> catalogId to items.toList() },
    liveChannels = liveChannels.toList(),
    iptvMovies = iptvMovies.toList(),
    iptvSeries = iptvSeries.toList(),
)
