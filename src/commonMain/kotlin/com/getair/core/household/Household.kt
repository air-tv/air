package com.getair.core.household

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class HouseholdProfileId(val value: String) {
    init { require(value.isNotBlank()) }
}

@Serializable
data class HouseholdProfile(
    val id: HouseholdProfileId,
    val name: String,
    val initials: String,
    val isKids: Boolean = false,
) {
    init {
        require(name.isNotBlank() && name.length <= 40)
        require(initials.isNotBlank() && initials.length <= 4)
    }
}

@Serializable
enum class StreamQuality { Auto, Uhd, FullHd, Hd, Sd }

@Serializable
enum class LiveTvBuffer { LowLatency, Balanced, Stable }

@Serializable
enum class ResumePolicy { Ask, Resume, Restart }

@Serializable
enum class PosterDensity { Compact, Comfortable, Spacious }

@Serializable
enum class DecoderPolicy { PreferPlatform, PreferCompatibility }

@Serializable
data class ProfilePreferences(
    val autoplayNextEpisode: Boolean = true,
    val streamQuality: StreamQuality = StreamQuality.Auto,
    val liveTvBuffer: LiveTvBuffer = LiveTvBuffer.Balanced,
    val resumePolicy: ResumePolicy = ResumePolicy.Ask,
    val subtitlesEnabled: Boolean = true,
    val preferredSubtitleLanguage: String = "en",
    val preferredAudioLanguage: String? = null,
    val showContentRatings: Boolean = true,
) {
    init {
        require(preferredSubtitleLanguage.isNotBlank())
        require(preferredAudioLanguage == null || preferredAudioLanguage.isNotBlank())
    }
}

@Serializable
data class DeviceSettings(
    val oledBlack: Boolean = false,
    val reduceMotion: Boolean = false,
    val posterDensity: PosterDensity = PosterDensity.Comfortable,
    val catalogRefreshMinutes: Int = 6 * 60,
    val localNetworkSources: Boolean = false,
    val hardwareDecoding: Boolean = true,
    val decoderPolicy: DecoderPolicy = DecoderPolicy.PreferPlatform,
    val networkTimeoutMillis: Long = 15_000,
    val maximumAddonResponseBytes: Int = 10 * 1024 * 1024,
    val diagnosticsOverlay: Boolean = false,
) {
    init {
        require(catalogRefreshMinutes > 0)
        require(networkTimeoutMillis > 0)
        require(maximumAddonResponseBytes > 0)
    }
}

@Serializable
data class HouseholdState(
    val profiles: List<HouseholdProfile> = emptyList(),
    val selectedProfileId: HouseholdProfileId? = null,
    val profilePreferences: Map<HouseholdProfileId, ProfilePreferences> = emptyMap(),
    val deviceSettings: DeviceSettings = DeviceSettings(),
    val revision: Long = 0,
) {
    init {
        require(revision >= 0)
        require(profiles.map(HouseholdProfile::id).distinct().size == profiles.size)
        require(profiles.map { it.name.lowercase() }.distinct().size == profiles.size)
        require(selectedProfileId == null || profiles.any { it.id == selectedProfileId })
        require(profilePreferences.keys.all { id -> profiles.any { it.id == id } })
    }

    val selectedProfile: HouseholdProfile?
        get() = profiles.firstOrNull { it.id == selectedProfileId }
}

interface LocalHouseholdStore {
    val state: StateFlow<HouseholdState>
    suspend fun replace(state: HouseholdState)
}

class InMemoryHouseholdStore(initial: HouseholdState = HouseholdState()) : LocalHouseholdStore {
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<HouseholdState> = mutableState.asStateFlow()

    override suspend fun replace(state: HouseholdState) {
        mutableState.value = state
    }
}

fun interface HouseholdSyncSource {
    suspend fun load(previous: HouseholdState): HouseholdState
}

sealed interface HouseholdRefreshResult {
    data object LocalOnly : HouseholdRefreshResult
    data class Updated(val revision: Long) : HouseholdRefreshResult
}

class LocalFirstHouseholdRepository(
    private val store: LocalHouseholdStore,
    private val syncSource: HouseholdSyncSource? = null,
) {
    private val commands = Mutex()
    val state: StateFlow<HouseholdState> = store.state

    suspend fun upsertProfile(
        profile: HouseholdProfile,
        preferences: ProfilePreferences = ProfilePreferences(),
    ) = update { previous ->
        val conflictingName = previous.profiles.any {
            it.id != profile.id && it.name.equals(profile.name, ignoreCase = true)
        }
        require(!conflictingName) { "Household profile names must be unique" }
        val existing = previous.profiles.indexOfFirst { it.id == profile.id }
        if (existing >= 0 && previous.profiles[existing] == profile) return@update previous
        val profiles = if (existing < 0) previous.profiles + profile else previous.profiles.toMutableList().also {
            it[existing] = profile
        }
        previous.copy(
            profiles = profiles,
            selectedProfileId = previous.selectedProfileId ?: profile.id,
            profilePreferences = previous.profilePreferences +
                (profile.id to (previous.profilePreferences[profile.id] ?: preferences)),
            revision = previous.revision + 1,
        )
    }

    suspend fun removeProfile(id: HouseholdProfileId) = update { previous ->
        if (previous.profiles.none { it.id == id }) return@update previous
        val remaining = previous.profiles.filterNot { it.id == id }
        previous.copy(
            profiles = remaining,
            selectedProfileId = if (previous.selectedProfileId == id) remaining.firstOrNull()?.id
            else previous.selectedProfileId,
            profilePreferences = previous.profilePreferences - id,
            revision = previous.revision + 1,
        )
    }

    suspend fun selectProfile(id: HouseholdProfileId) = update { previous ->
        require(previous.profiles.any { it.id == id }) { "Cannot select an unknown household profile" }
        if (previous.selectedProfileId == id) previous else previous.copy(
            selectedProfileId = id,
            revision = previous.revision + 1,
        )
    }

    suspend fun updateProfilePreferences(
        id: HouseholdProfileId,
        transform: (ProfilePreferences) -> ProfilePreferences,
    ) = update { previous ->
        require(previous.profiles.any { it.id == id }) { "Cannot update an unknown household profile" }
        val current = previous.profilePreferences[id] ?: ProfilePreferences()
        val next = transform(current)
        if (next == current) return@update previous
        previous.copy(
            profilePreferences = previous.profilePreferences + (id to next),
            revision = previous.revision + 1,
        )
    }

    suspend fun updateDeviceSettings(transform: (DeviceSettings) -> DeviceSettings) = update { previous ->
        val next = transform(previous.deviceSettings)
        if (next == previous.deviceSettings) previous
        else previous.copy(deviceSettings = next, revision = previous.revision + 1)
    }

    suspend fun refresh(): HouseholdRefreshResult = commands.withLock {
        val source = syncSource ?: return@withLock HouseholdRefreshResult.LocalOnly
        val next = source.load(state.value)
        store.replace(next)
        HouseholdRefreshResult.Updated(next.revision)
    }

    private suspend fun update(transform: (HouseholdState) -> HouseholdState) = commands.withLock {
        store.replace(transform(state.value))
    }
}
