package com.getair.core.history

import com.getair.core.household.HouseholdProfileId
import com.getair.iptv.model.EpisodeId
import com.getair.iptv.model.MovieId
import com.getair.iptv.model.SeriesId
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable

@Serializable
sealed interface OnDemandContentRef {
    @Serializable
    data class Stremio(val type: String, val id: String) : OnDemandContentRef {
        init {
            require(type.isNotBlank())
            require(id.isNotBlank())
        }
    }

    @Serializable
    data class IptvMovie(val id: MovieId) : OnDemandContentRef

    @Serializable
    data class IptvEpisode(
        val id: EpisodeId,
        val seriesId: SeriesId,
        val season: Int,
        val episode: Int,
    ) : OnDemandContentRef {
        init {
            require(season >= 0)
            require(episode >= 0)
        }
    }
}

@Serializable
data class WatchProgress(
    val content: OnDemandContentRef,
    val positionMillis: Long,
    val durationMillis: Long,
    val updatedAtEpochMillis: Long,
) {
    init {
        require(positionMillis >= 0)
        require(durationMillis > 0)
        require(updatedAtEpochMillis >= 0)
    }

    val fraction: Float
        get() = (positionMillis.toDouble() / durationMillis).coerceIn(0.0, 1.0).toFloat()
}

@Serializable
data class ContinueWatchingState(
    val entriesByProfile: Map<HouseholdProfileId, List<WatchProgress>> = emptyMap(),
    val revision: Long = 0,
) {
    init {
        require(revision >= 0)
        require(entriesByProfile.values.all { entries ->
            entries.map(WatchProgress::content).distinct().size == entries.size
        })
    }
}

data class ContinueWatchingOptions(
    val maximumEntriesPerProfile: Int = 100,
    val completionThreshold: Float = 0.95f,
) {
    init {
        require(maximumEntriesPerProfile > 0)
        require(completionThreshold > 0f && completionThreshold <= 1f)
    }
}

interface LocalContinueWatchingStore {
    val state: StateFlow<ContinueWatchingState>
    suspend fun replace(state: ContinueWatchingState)
}

class InMemoryContinueWatchingStore(
    initial: ContinueWatchingState = ContinueWatchingState(),
) : LocalContinueWatchingStore {
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<ContinueWatchingState> = mutableState.asStateFlow()
    override suspend fun replace(state: ContinueWatchingState) {
        mutableState.value = state
    }
}

fun interface ContinueWatchingSyncSource {
    suspend fun load(previous: ContinueWatchingState): ContinueWatchingState
}

sealed interface ContinueWatchingRefreshResult {
    data object LocalOnly : ContinueWatchingRefreshResult
    data class Updated(val revision: Long) : ContinueWatchingRefreshResult
}

class LocalFirstContinueWatchingRepository(
    private val store: LocalContinueWatchingStore,
    private val options: ContinueWatchingOptions = ContinueWatchingOptions(),
    private val syncSource: ContinueWatchingSyncSource? = null,
) {
    private val commands = Mutex()
    val state: StateFlow<ContinueWatchingState> = store.state

    fun entries(profileId: HouseholdProfileId): List<WatchProgress> =
        state.value.entriesByProfile[profileId].orEmpty()

    suspend fun record(profileId: HouseholdProfileId, progress: WatchProgress) = commands.withLock {
        val previous = state.value
        val current = previous.entriesByProfile[profileId].orEmpty()
        val withoutCurrent = current.filterNot { it.content == progress.content }
        val nextEntries = if (progress.fraction >= options.completionThreshold) {
            withoutCurrent
        } else {
            (withoutCurrent + progress)
                .sortedByDescending(WatchProgress::updatedAtEpochMillis)
                .take(options.maximumEntriesPerProfile)
        }
        if (nextEntries == current) return@withLock
        val byProfile = if (nextEntries.isEmpty()) previous.entriesByProfile - profileId
        else previous.entriesByProfile + (profileId to nextEntries)
        store.replace(previous.copy(entriesByProfile = byProfile, revision = previous.revision + 1))
    }

    suspend fun remove(profileId: HouseholdProfileId, content: OnDemandContentRef) = commands.withLock {
        val previous = state.value
        val current = previous.entriesByProfile[profileId].orEmpty()
        val nextEntries = current.filterNot { it.content == content }
        if (nextEntries.size == current.size) return@withLock
        val byProfile = if (nextEntries.isEmpty()) previous.entriesByProfile - profileId
        else previous.entriesByProfile + (profileId to nextEntries)
        store.replace(previous.copy(entriesByProfile = byProfile, revision = previous.revision + 1))
    }

    suspend fun clear(profileId: HouseholdProfileId) = commands.withLock {
        val previous = state.value
        if (profileId !in previous.entriesByProfile) return@withLock
        store.replace(
            previous.copy(
                entriesByProfile = previous.entriesByProfile - profileId,
                revision = previous.revision + 1,
            ),
        )
    }

    suspend fun refresh(): ContinueWatchingRefreshResult = commands.withLock {
        val source = syncSource ?: return@withLock ContinueWatchingRefreshResult.LocalOnly
        val next = source.load(state.value)
        store.replace(next)
        ContinueWatchingRefreshResult.Updated(next.revision)
    }
}
