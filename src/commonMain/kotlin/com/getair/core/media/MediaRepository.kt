package com.getair.core.media

import com.getair.iptv.model.IptvChannel
import com.getair.iptv.model.IptvMovie
import com.getair.iptv.model.IptvSeries
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

data class MediaSnapshot(
    val stremioCatalogs: Map<String, List<MetaPreview>> = emptyMap(),
    val liveChannels: List<IptvChannel> = emptyList(),
    val iptvMovies: List<IptvMovie> = emptyList(),
    val iptvSeries: List<IptvSeries> = emptyList(),
    val revision: Long = 0,
)

interface LocalMediaStore {
    val snapshot: StateFlow<MediaSnapshot>
    suspend fun replace(snapshot: MediaSnapshot)
}

class InMemoryMediaStore(initial: MediaSnapshot = MediaSnapshot()) : LocalMediaStore {
    private val mutableSnapshot = MutableStateFlow(initial)
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
    data class Updated(val revision: Long) : MediaRefreshResult
}

class LocalFirstMediaRepository(
    private val store: LocalMediaStore,
    private val syncSource: MediaSyncSource? = null,
) {
    val snapshot: StateFlow<MediaSnapshot> = store.snapshot

    suspend fun replaceLocal(snapshot: MediaSnapshot) {
        store.replace(snapshot)
    }

    suspend fun refresh(): MediaRefreshResult {
        val source = syncSource ?: return MediaRefreshResult.LocalOnly
        val next = source.load(snapshot.value)
        store.replace(next)
        return MediaRefreshResult.Updated(next.revision)
    }
}
