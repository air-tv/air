package com.getair.core.source

import com.getair.iptv.StalkerCredentials
import com.getair.iptv.XtreamCredentials
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class LocalSourceId(val value: String) {
    init { require(value.isNotBlank()) }
}

@Serializable
enum class LocalSourceKind { Xtream, Stalker, M3u, StremioAddon }

@Serializable
data class LocalSourceProfile(
    val id: LocalSourceId,
    val name: String,
    val kind: LocalSourceKind,
    val enabled: Boolean = true,
) {
    init { require(name.isNotBlank() && name.length <= 80) }
}

sealed interface LocalSourceSecret {
    val kind: LocalSourceKind
}

class XtreamSourceSecret(
    val credentials: XtreamCredentials,
) : LocalSourceSecret {
    override val kind: LocalSourceKind = LocalSourceKind.Xtream
    override fun toString(): String = "XtreamSourceSecret(credentials=<redacted>)"
}

class StalkerSourceSecret(
    val credentials: StalkerCredentials,
) : LocalSourceSecret {
    override val kind: LocalSourceKind = LocalSourceKind.Stalker
    override fun toString(): String = "StalkerSourceSecret(credentials=<redacted>)"
}

class M3uSourceSecret(
    val playlistUrl: String,
    val xmltvUrl: String? = null,
    val headers: Map<String, String> = emptyMap(),
) : LocalSourceSecret {
    init {
        require(playlistUrl.isNotBlank())
        require(xmltvUrl == null || xmltvUrl.isNotBlank())
    }

    override val kind: LocalSourceKind = LocalSourceKind.M3u
    override fun toString(): String =
        "M3uSourceSecret(playlistUrl=<redacted>, xmltvUrl=<redacted>, headers=<redacted>)"
}

class StremioAddonSourceSecret(
    val manifestUrl: String,
    val headers: Map<String, String> = emptyMap(),
) : LocalSourceSecret {
    init { require(manifestUrl.isNotBlank()) }
    override val kind: LocalSourceKind = LocalSourceKind.StremioAddon
    override fun toString(): String =
        "StremioAddonSourceSecret(manifestUrl=<redacted>, headers=<redacted>)"
}

@Serializable
data class LocalSourceState(
    val profiles: List<LocalSourceProfile> = emptyList(),
    val revision: Long = 0,
) {
    init {
        require(revision >= 0)
        require(profiles.map(LocalSourceProfile::id).distinct().size == profiles.size)
        require(profiles.map { it.name.lowercase() }.distinct().size == profiles.size)
    }
}

interface LocalSourceStore {
    val state: StateFlow<LocalSourceState>
    suspend fun replace(state: LocalSourceState)
}

class InMemoryLocalSourceStore(initial: LocalSourceState = LocalSourceState()) : LocalSourceStore {
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<LocalSourceState> = mutableState.asStateFlow()
    override suspend fun replace(state: LocalSourceState) {
        mutableState.value = state
    }
}

interface LocalSourceSecretStore {
    suspend fun read(id: LocalSourceId): LocalSourceSecret?
    suspend fun write(id: LocalSourceId, secret: LocalSourceSecret)
    suspend fun remove(id: LocalSourceId)
}

/** Tests and ephemeral sessions only. Production shells inject an OS-backed vault. */
class InMemoryLocalSourceSecretStore : LocalSourceSecretStore {
    private val mutex = Mutex()
    private val values = mutableMapOf<LocalSourceId, LocalSourceSecret>()
    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock { values[id] }
    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        mutex.withLock { values[id] = secret }
    }
    override suspend fun remove(id: LocalSourceId) {
        mutex.withLock { values.remove(id) }
    }
    override fun toString(): String = "InMemoryLocalSourceSecretStore(values=<redacted>)"
}

fun interface SourceMetadataSyncSource {
    suspend fun load(previous: LocalSourceState): LocalSourceState
}

sealed interface SourceRefreshResult {
    data object LocalOnly : SourceRefreshResult
    data class Updated(val revision: Long) : SourceRefreshResult
}

class LocalSourceRegistry(
    private val store: LocalSourceStore,
    private val secrets: LocalSourceSecretStore,
    private val syncSource: SourceMetadataSyncSource? = null,
) {
    private val commands = Mutex()
    val state: StateFlow<LocalSourceState> = store.state

    suspend fun upsert(profile: LocalSourceProfile, secret: LocalSourceSecret) = commands.withLock {
        require(profile.kind == secret.kind) { "Source metadata and credential kind must match" }
        val previous = state.value
        val replaced = previous.profiles.filter {
            it.id == profile.id || it.name.equals(profile.name, ignoreCase = true)
        }
        if (replaced.size == 1 && replaced.single() == profile && secrets.read(profile.id) === secret) {
            return@withLock
        }
        secrets.write(profile.id, secret)
        val retained = previous.profiles - replaced.toSet()
        store.replace(previous.copy(profiles = retained + profile, revision = previous.revision + 1))
        for (old in replaced) {
            if (old.id != profile.id) secrets.remove(old.id)
        }
    }

    suspend fun setEnabled(id: LocalSourceId, enabled: Boolean) = commands.withLock {
        val previous = state.value
        val index = previous.profiles.indexOfFirst { it.id == id }
        require(index >= 0) { "Cannot update an unknown media source" }
        if (previous.profiles[index].enabled == enabled) return@withLock
        val profiles = previous.profiles.toMutableList().also {
            it[index] = it[index].copy(enabled = enabled)
        }
        store.replace(previous.copy(profiles = profiles, revision = previous.revision + 1))
    }

    suspend fun secret(id: LocalSourceId): LocalSourceSecret? = commands.withLock {
        if (state.value.profiles.none { it.id == id }) return@withLock null
        secrets.read(id)
    }

    suspend fun remove(id: LocalSourceId) = commands.withLock {
        val previous = state.value
        if (previous.profiles.none { it.id == id }) return@withLock
        store.replace(
            previous.copy(
                profiles = previous.profiles.filterNot { it.id == id },
                revision = previous.revision + 1,
            ),
        )
        secrets.remove(id)
    }

    suspend fun refreshMetadata(): SourceRefreshResult = commands.withLock {
        val source = syncSource ?: return@withLock SourceRefreshResult.LocalOnly
        val next = source.load(state.value)
        require(next.profiles.all { incoming ->
            val current = state.value.profiles.firstOrNull { it.id == incoming.id }
            current == null || current.kind == incoming.kind
        }) { "Synced source metadata cannot change a credential kind" }
        store.replace(next)
        SourceRefreshResult.Updated(next.revision)
    }

    override fun toString(): String =
        "LocalSourceRegistry(store=<redacted>, secrets=<redacted>, syncSource=${syncSource != null})"
}
