package com.getair.core.library

import com.getair.core.household.HouseholdProfileId
import com.getair.core.persistence.LocalDocumentStore
import com.getair.core.persistence.validateLocalDocumentName
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Bounded local-document implementation. This is intentionally not the future
 * catalog/guide store: every collection has a hard item limit and the encoded
 * document itself has a fixed maximum size.
 */
class PersistentProfileLibraryStore private constructor(
    private val documents: LocalDocumentStore,
    private val document: String,
    initial: ProfileLibraryState,
) : LocalProfileLibraryStore {
    private val commands = Mutex()
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<ProfileLibraryState> = mutableState.asStateFlow()

    override suspend fun replace(state: ProfileLibraryState) = commands.withLock {
        if (state == mutableState.value) return@withLock
        val encoded = profileLibraryJson.encodeToString(
            StoredProfileLibraryDocument.serializer(),
            StoredProfileLibraryDocument(CURRENT_DOCUMENT_VERSION, state),
        )
        require(encoded.length <= MAXIMUM_DOCUMENT_CHARS) {
            "Local profile library document is too large"
        }
        documents.write(document, encoded)
        mutableState.value = state
    }

    override fun toString(): String = "PersistentProfileLibraryStore(document=<redacted>)"

    companion object {
        suspend fun open(
            documents: LocalDocumentStore,
            document: String = "profile-library.v1",
            initial: ProfileLibraryState = ProfileLibraryState(),
        ): PersistentProfileLibraryStore {
            validateLocalDocumentName(document)
            val value = documents.read(document)?.let(::decodeProfileLibraryDocument) ?: initial
            return PersistentProfileLibraryStore(documents, document, value)
        }
    }
}

@Serializable
private data class StoredProfileLibraryDocument(
    val version: Int,
    val value: ProfileLibraryState,
)

@Serializable
private data class DocumentVersion(val version: Int)

/** Prototype schema retained solely for deterministic one-way migration. */
@Serializable
private data class LegacyProfileLibraryStateV0(
    val favoritesByProfile: Map<HouseholdProfileId, List<FavoriteEntry>> = emptyMap(),
    val liveHistoryByProfile: Map<HouseholdProfileId, List<LiveChannelVisit>> = emptyMap(),
    val revision: Long = 0,
)

@Serializable
private data class LegacyProfileLibraryDocumentV0(
    val version: Int,
    val value: LegacyProfileLibraryStateV0,
)

private fun decodeProfileLibraryDocument(encoded: String): ProfileLibraryState {
    require(encoded.length <= MAXIMUM_DOCUMENT_CHARS) { "Local profile library document is too large" }
    return try {
        when (profileLibraryJson.decodeFromString(DocumentVersion.serializer(), encoded).version) {
            CURRENT_DOCUMENT_VERSION -> profileLibraryJson.decodeFromString(
                StoredProfileLibraryDocument.serializer(),
                encoded,
            ).value
            LEGACY_DOCUMENT_VERSION -> profileLibraryJson.decodeFromString(
                LegacyProfileLibraryDocumentV0.serializer(),
                encoded,
            ).value.migrate()
            else -> throw IllegalArgumentException("Unsupported local profile library document version")
        }
    } catch (_: Exception) {
        // Decoder errors may contain malformed document text. Do not retain the
        // cause or any content in the public failure.
        throw IllegalStateException("Local profile library document is invalid")
    }
}

private fun LegacyProfileLibraryStateV0.migrate(): ProfileLibraryState {
    val profileIds = favoritesByProfile.keys + liveHistoryByProfile.keys
    return ProfileLibraryState(
        profiles = profileIds.associateWith { profileId ->
            ProfileLibrary(
                favorites = favoritesByProfile[profileId].orEmpty(),
                liveChannelHistory = liveHistoryByProfile[profileId].orEmpty(),
            )
        },
        revision = revision,
    )
}

private val profileLibraryJson = Json {
    encodeDefaults = true
    explicitNulls = false
    ignoreUnknownKeys = true
    classDiscriminator = "_type"
}

private const val LEGACY_DOCUMENT_VERSION = 0
private const val CURRENT_DOCUMENT_VERSION = 1
private const val MAXIMUM_DOCUMENT_CHARS = 4 * 1024 * 1024
