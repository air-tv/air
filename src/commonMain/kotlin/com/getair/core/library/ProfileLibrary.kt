package com.getair.core.library

import com.getair.core.history.OnDemandContentRef
import com.getair.core.household.HouseholdProfileId
import com.getair.core.household.LiveTvBuffer
import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.ChannelId
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * A durable media identity. It deliberately contains no playback URL, request
 * header, provider credential, display metadata, or artwork bytes.
 *
 * IPTV identifiers are source-qualified because providers routinely reuse
 * numeric IDs. Stremio IDs may be source-independent so that the same title is
 * one favorite even when several addons can serve it.
 */
@Serializable
sealed interface LibraryItemRef {
    @Serializable
    @SerialName("on_demand")
    data class OnDemand(
        val content: OnDemandContentRef,
        val sourceId: LocalSourceId? = null,
    ) : LibraryItemRef {
        init {
            require((content is OnDemandContentRef.Stremio) == (sourceId == null)) {
                "Stremio references are global; IPTV references must identify their local source"
            }
            sourceId?.value?.requireOpaqueIdentifier("Local source ID")
            content.validateDurableIdentity()
        }
    }

    @Serializable
    @SerialName("live_channel")
    data class LiveChannel(
        val sourceId: LocalSourceId,
        val channelId: ChannelId,
    ) : LibraryItemRef {
        init {
            sourceId.value.requireOpaqueIdentifier("Local source ID")
            channelId.value.requireOpaqueIdentifier("Channel ID")
        }
    }
}

@Serializable
data class FavoriteEntry(
    val item: LibraryItemRef,
    val addedAtEpochMillis: Long,
) {
    init { require(addedAtEpochMillis >= 0) }
}

@Serializable
data class LiveChannelVisit(
    val channel: LibraryItemRef.LiveChannel,
    val viewedAtEpochMillis: Long,
) {
    init { require(viewedAtEpochMillis >= 0) }
}

/** Concrete defaults used when no narrower override exists. */
@Serializable
data class PlaybackPreferenceDefaults(
    val preferredAudioLanguage: String? = null,
    val preferredSubtitleLanguage: String? = "en",
    val subtitlesEnabled: Boolean = true,
    val liveTvBuffer: LiveTvBuffer = LiveTvBuffer.Balanced,
) {
    init {
        preferredAudioLanguage?.requireLanguageTag()
        preferredSubtitleLanguage?.requireLanguageTag()
    }
}

/** Null means inherit the next broader profile/global value. */
@Serializable
data class PlaybackPreferenceOverrides(
    val preferredAudioLanguage: String? = null,
    val preferredSubtitleLanguage: String? = null,
    val subtitlesEnabled: Boolean? = null,
    val liveTvBuffer: LiveTvBuffer? = null,
) {
    init {
        preferredAudioLanguage?.requireLanguageTag()
        preferredSubtitleLanguage?.requireLanguageTag()
    }

    val isEmpty: Boolean
        get() = preferredAudioLanguage == null &&
            preferredSubtitleLanguage == null &&
            subtitlesEnabled == null &&
            liveTvBuffer == null
}

@Serializable
data class ItemPlaybackPreference(
    val item: LibraryItemRef,
    val overrides: PlaybackPreferenceOverrides,
) {
    init { require(!overrides.isEmpty) { "Empty item playback preferences must not be stored" } }
}

@Serializable
data class ProfileLibrary(
    val playbackDefaults: PlaybackPreferenceOverrides = PlaybackPreferenceOverrides(),
    val favorites: List<FavoriteEntry> = emptyList(),
    val liveChannelHistory: List<LiveChannelVisit> = emptyList(),
    val itemPlaybackPreferences: List<ItemPlaybackPreference> = emptyList(),
) {
    init {
        require(favorites.size <= HARD_MAXIMUM_FAVORITES_PER_PROFILE)
        require(liveChannelHistory.size <= HARD_MAXIMUM_LIVE_HISTORY_PER_PROFILE)
        require(itemPlaybackPreferences.size <= HARD_MAXIMUM_ITEM_PREFERENCES_PER_PROFILE)
        require(favorites.map(FavoriteEntry::item).distinct().size == favorites.size)
        require(liveChannelHistory.map(LiveChannelVisit::channel).distinct().size == liveChannelHistory.size)
        require(itemPlaybackPreferences.map(ItemPlaybackPreference::item).distinct().size ==
            itemPlaybackPreferences.size)
    }

    internal val isEmpty: Boolean
        get() = playbackDefaults.isEmpty && favorites.isEmpty() &&
            liveChannelHistory.isEmpty() && itemPlaybackPreferences.isEmpty()
}

@Serializable
data class ProfileLibraryState(
    val globalPlaybackDefaults: PlaybackPreferenceDefaults = PlaybackPreferenceDefaults(),
    val profiles: Map<HouseholdProfileId, ProfileLibrary> = emptyMap(),
    val revision: Long = 0,
) {
    init {
        require(revision >= 0)
        require(profiles.size <= HARD_MAXIMUM_PROFILES)
        require(profiles.keys.all { id ->
            id.value.length <= MAXIMUM_PROFILE_ID_CHARS && id.value.none(Char::isISOControl)
        }) { "Profile library profile IDs must be bounded opaque identifiers" }
    }
}

data class ProfileLibraryOptions(
    val maximumProfiles: Int = 32,
    val maximumFavoritesPerProfile: Int = 500,
    val maximumLiveHistoryPerProfile: Int = 100,
    val maximumItemPreferencesPerProfile: Int = 500,
) {
    init {
        require(maximumProfiles in 1..HARD_MAXIMUM_PROFILES)
        require(maximumFavoritesPerProfile in 1..HARD_MAXIMUM_FAVORITES_PER_PROFILE)
        require(maximumLiveHistoryPerProfile in 1..HARD_MAXIMUM_LIVE_HISTORY_PER_PROFILE)
        require(maximumItemPreferencesPerProfile in 1..HARD_MAXIMUM_ITEM_PREFERENCES_PER_PROFILE)
    }
}

interface LocalProfileLibraryStore {
    val state: StateFlow<ProfileLibraryState>
    suspend fun replace(state: ProfileLibraryState)
}

class InMemoryProfileLibraryStore(
    initial: ProfileLibraryState = ProfileLibraryState(),
) : LocalProfileLibraryStore {
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<ProfileLibraryState> = mutableState.asStateFlow()

    override suspend fun replace(state: ProfileLibraryState) {
        mutableState.value = state
    }
}

fun interface ProfileLibrarySyncSource {
    suspend fun load(previous: ProfileLibraryState): ProfileLibraryState
}

sealed interface ProfileLibraryRefreshResult {
    data object LocalOnly : ProfileLibraryRefreshResult
    data class Unchanged(val revision: Long) : ProfileLibraryRefreshResult
    data class Updated(val revision: Long) : ProfileLibraryRefreshResult
}

class LocalFirstProfileLibraryRepository(
    private val store: LocalProfileLibraryStore,
    private val options: ProfileLibraryOptions = ProfileLibraryOptions(),
    private val syncSource: ProfileLibrarySyncSource? = null,
) {
    private val commands = Mutex()
    val state: StateFlow<ProfileLibraryState> = store.state

    init { state.value.requireWithin(options) }

    fun profile(id: HouseholdProfileId): ProfileLibrary =
        state.value.profiles[id] ?: ProfileLibrary()

    fun resolvedPlaybackPreferences(
        profileId: HouseholdProfileId,
        item: LibraryItemRef? = null,
    ): PlaybackPreferenceDefaults {
        val snapshot = state.value
        val profile = snapshot.profiles[profileId]
        val itemOverrides = item?.let { wanted ->
            profile?.itemPlaybackPreferences?.firstOrNull { it.item == wanted }?.overrides
        }
        return snapshot.globalPlaybackDefaults
            .apply(profile?.playbackDefaults)
            .apply(itemOverrides)
    }

    suspend fun setGlobalPlaybackDefaults(defaults: PlaybackPreferenceDefaults) = update { previous ->
        val normalized = defaults.normalized()
        if (previous.globalPlaybackDefaults == normalized) previous
        else previous.copy(globalPlaybackDefaults = normalized, revision = previous.nextRevision())
    }

    suspend fun setProfilePlaybackDefaults(
        profileId: HouseholdProfileId,
        overrides: PlaybackPreferenceOverrides,
    ) = updateProfile(profileId) { profile ->
        profile.copy(playbackDefaults = overrides.normalized())
    }

    suspend fun setItemPlaybackPreferences(
        profileId: HouseholdProfileId,
        item: LibraryItemRef,
        overrides: PlaybackPreferenceOverrides,
    ) = updateProfile(profileId) { profile ->
        val normalized = overrides.normalized()
        val exists = profile.itemPlaybackPreferences.any { it.item == item }
        if (!normalized.isEmpty && !exists &&
            profile.itemPlaybackPreferences.size >= options.maximumItemPreferencesPerProfile
        ) {
            throw IllegalStateException("Local item playback preference limit reached")
        }
        val withoutItem = profile.itemPlaybackPreferences.filterNot { it.item == item }
        profile.copy(
            itemPlaybackPreferences = if (normalized.isEmpty) {
                withoutItem
            } else {
                (withoutItem + ItemPlaybackPreference(item, normalized))
                    .sortedBy { it.item.identityKey() }
            },
        )
    }

    suspend fun addFavorite(
        profileId: HouseholdProfileId,
        item: LibraryItemRef,
        addedAtEpochMillis: Long,
    ) = updateProfile(profileId) { profile ->
        val entry = FavoriteEntry(item, addedAtEpochMillis)
        val current = profile.favorites.firstOrNull { it.item == item }
        if (current == entry) profile else profile.copy(
            favorites = (profile.favorites.filterNot { it.item == item } + entry)
                .sortedWith(compareByDescending<FavoriteEntry> { it.addedAtEpochMillis }
                    .thenBy { it.item.identityKey() })
                .take(options.maximumFavoritesPerProfile),
        )
    }

    suspend fun removeFavorite(profileId: HouseholdProfileId, item: LibraryItemRef) =
        updateProfile(profileId) { profile ->
            profile.copy(favorites = profile.favorites.filterNot { it.item == item })
        }

    suspend fun recordLiveChannel(
        profileId: HouseholdProfileId,
        channel: LibraryItemRef.LiveChannel,
        viewedAtEpochMillis: Long,
    ) = updateProfile(profileId) { profile ->
        val visit = LiveChannelVisit(channel, viewedAtEpochMillis)
        val current = profile.liveChannelHistory.firstOrNull { it.channel == channel }
        if (current == visit) profile else profile.copy(
            liveChannelHistory = (profile.liveChannelHistory.filterNot { it.channel == channel } + visit)
                .sortedWith(compareByDescending<LiveChannelVisit> { it.viewedAtEpochMillis }
                    .thenBy { it.channel.identityKey() })
                .take(options.maximumLiveHistoryPerProfile),
        )
    }

    suspend fun clearLiveChannelHistory(profileId: HouseholdProfileId) =
        updateProfile(profileId) { it.copy(liveChannelHistory = emptyList()) }

    /** Idempotent cleanup entry point for household profile deletion. */
    suspend fun removeProfile(profileId: HouseholdProfileId) = update { previous ->
        if (profileId !in previous.profiles) previous else previous.copy(
            profiles = previous.profiles - profileId,
            revision = previous.nextRevision(),
        )
    }

    suspend fun refresh(): ProfileLibraryRefreshResult {
        val source = syncSource ?: return ProfileLibraryRefreshResult.LocalOnly
        return commands.withLock {
            val previous = state.value
            val incoming = source.load(previous).bounded(options)
            val comparable = incoming.copy(revision = previous.revision)
            if (comparable == previous) {
                ProfileLibraryRefreshResult.Unchanged(previous.revision)
            } else {
                val next = comparable.copy(revision = previous.nextRevision())
                store.replace(next)
                ProfileLibraryRefreshResult.Updated(next.revision)
            }
        }
    }

    private suspend fun updateProfile(
        profileId: HouseholdProfileId,
        transform: (ProfileLibrary) -> ProfileLibrary,
    ) = update { previous ->
        require(profileId.value.length <= MAXIMUM_PROFILE_ID_CHARS &&
            profileId.value.none(Char::isISOControl)
        ) { "Profile library profile IDs must be bounded opaque identifiers" }
        val existed = profileId in previous.profiles
        val current = previous.profiles[profileId] ?: ProfileLibrary()
        val next = transform(current).bounded(options)
        if (!existed && !next.isEmpty && previous.profiles.size >= options.maximumProfiles) {
            throw IllegalStateException("Local profile library profile limit reached")
        }
        if (next == current) previous else previous.copy(
            profiles = if (next.isEmpty) previous.profiles - profileId
            else previous.profiles + (profileId to next),
            revision = previous.nextRevision(),
        )
    }

    private suspend fun update(transform: (ProfileLibraryState) -> ProfileLibraryState) =
        commands.withLock {
            val previous = state.value
            val next = transform(previous)
            if (next != previous) store.replace(next)
        }
}

private fun PlaybackPreferenceDefaults.apply(
    overrides: PlaybackPreferenceOverrides?,
): PlaybackPreferenceDefaults = if (overrides == null) this else copy(
    preferredAudioLanguage = overrides.preferredAudioLanguage ?: preferredAudioLanguage,
    preferredSubtitleLanguage = overrides.preferredSubtitleLanguage ?: preferredSubtitleLanguage,
    subtitlesEnabled = overrides.subtitlesEnabled ?: subtitlesEnabled,
    liveTvBuffer = overrides.liveTvBuffer ?: liveTvBuffer,
)

private fun PlaybackPreferenceDefaults.normalized(): PlaybackPreferenceDefaults = copy(
    preferredAudioLanguage = preferredAudioLanguage?.normalizedLanguageTag(),
    preferredSubtitleLanguage = preferredSubtitleLanguage?.normalizedLanguageTag(),
)

private fun PlaybackPreferenceOverrides.normalized(): PlaybackPreferenceOverrides = copy(
    preferredAudioLanguage = preferredAudioLanguage?.normalizedLanguageTag(),
    preferredSubtitleLanguage = preferredSubtitleLanguage?.normalizedLanguageTag(),
)

private fun ProfileLibrary.bounded(options: ProfileLibraryOptions): ProfileLibrary = copy(
    playbackDefaults = playbackDefaults.normalized(),
    favorites = favorites
        .distinctBy(FavoriteEntry::item)
        .sortedWith(compareByDescending<FavoriteEntry> { it.addedAtEpochMillis }
            .thenBy { it.item.identityKey() })
        .take(options.maximumFavoritesPerProfile),
    liveChannelHistory = liveChannelHistory
        .distinctBy(LiveChannelVisit::channel)
        .sortedWith(compareByDescending<LiveChannelVisit> { it.viewedAtEpochMillis }
            .thenBy { it.channel.identityKey() })
        .take(options.maximumLiveHistoryPerProfile),
    itemPlaybackPreferences = itemPlaybackPreferences
        .filterNot { it.overrides.isEmpty }
        .distinctBy(ItemPlaybackPreference::item)
        .sortedBy { it.item.identityKey() }
        .take(options.maximumItemPreferencesPerProfile),
)

private fun ProfileLibraryState.bounded(options: ProfileLibraryOptions): ProfileLibraryState = copy(
    globalPlaybackDefaults = globalPlaybackDefaults.normalized(),
    profiles = profiles.entries
        .sortedBy { it.key.value }
        .take(options.maximumProfiles)
        .mapNotNull { (id, value) -> value.bounded(options).takeUnless(ProfileLibrary::isEmpty)?.let { id to it } }
        .toMap(),
)

private fun ProfileLibraryState.requireWithin(options: ProfileLibraryOptions) {
    require(profiles.size <= options.maximumProfiles)
    profiles.values.forEach { profile ->
        require(profile.favorites.size <= options.maximumFavoritesPerProfile)
        require(profile.liveChannelHistory.size <= options.maximumLiveHistoryPerProfile)
        require(profile.itemPlaybackPreferences.size <= options.maximumItemPreferencesPerProfile)
    }
}

private fun ProfileLibraryState.nextRevision(): Long {
    check(revision < Long.MAX_VALUE) { "Profile library revision is exhausted" }
    return revision + 1
}

private fun LibraryItemRef.identityKey(): String = when (this) {
    is LibraryItemRef.LiveChannel -> key("live", sourceId.value, channelId.value)
    is LibraryItemRef.OnDemand -> when (val value = content) {
        is OnDemandContentRef.Stremio -> key("stremio", value.type, value.id)
        is OnDemandContentRef.IptvMovie -> key("iptv-movie", requireNotNull(sourceId).value, value.id.value)
        is OnDemandContentRef.IptvEpisode -> key(
            "iptv-episode",
            requireNotNull(sourceId).value,
            value.seriesId.value,
            value.id.value,
            value.season.toString(),
            value.episode.toString(),
        )
    }
}

private fun key(vararg parts: String): String = parts.joinToString(separator = "") { "${it.length}:$it" }

private fun OnDemandContentRef.validateDurableIdentity() = when (this) {
    is OnDemandContentRef.Stremio -> {
        type.requireOpaqueIdentifier("Stremio type")
        id.requireOpaqueIdentifier("Stremio ID")
    }
    is OnDemandContentRef.IptvMovie -> id.value.requireOpaqueIdentifier("IPTV movie ID")
    is OnDemandContentRef.IptvEpisode -> {
        id.value.requireOpaqueIdentifier("IPTV episode ID")
        seriesId.value.requireOpaqueIdentifier("IPTV series ID")
    }
}

private fun String.requireOpaqueIdentifier(label: String) {
    require(length <= MAXIMUM_IDENTIFIER_CHARS && none(Char::isISOControl) && "://" !in this) {
        "$label must be an opaque bounded identifier"
    }
}

private fun String.requireLanguageTag() {
    require(LANGUAGE_TAG.matches(this)) { "Language preference must be a BCP-47-like tag" }
}

private fun String.normalizedLanguageTag(): String = lowercase()

private const val MAXIMUM_IDENTIFIER_CHARS = 512
private const val MAXIMUM_PROFILE_ID_CHARS = 128
private const val HARD_MAXIMUM_PROFILES = 64
private const val HARD_MAXIMUM_FAVORITES_PER_PROFILE = 1_000
private const val HARD_MAXIMUM_LIVE_HISTORY_PER_PROFILE = 500
private const val HARD_MAXIMUM_ITEM_PREFERENCES_PER_PROFILE = 1_000
private val LANGUAGE_TAG = Regex("[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*")
