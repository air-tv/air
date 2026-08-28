package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.history.ContinueWatchingSyncSource
import com.getair.core.history.LocalFirstContinueWatchingRepository
import com.getair.core.household.HouseholdProfileId
import com.getair.core.household.HouseholdSyncSource
import com.getair.core.household.LocalFirstHouseholdRepository
import com.getair.core.library.LocalFirstProfileLibraryRepository
import com.getair.core.library.PersistentProfileLibraryStore
import com.getair.core.library.ProfileLibraryOptions
import com.getair.core.library.ProfileLibrarySyncSource
import com.getair.core.source.LocalSourceRegistry
import com.getair.core.source.LocalSourceSecretStore
import com.getair.core.source.SourceMetadataSyncSource

/** Optional future-server seams; ordinary local construction uses [None]. */
class LocalApplicationSyncSources(
    val household: HouseholdSyncSource? = null,
    val sourceMetadata: SourceMetadataSyncSource? = null,
    val continueWatching: ContinueWatchingSyncSource? = null,
    val profileLibrary: ProfileLibrarySyncSource? = null,
) {
    companion object {
        val None = LocalApplicationSyncSources()
    }
}

/** The local-first repositories needed by an application shell. */
class LocalApplicationState internal constructor(
    val household: LocalFirstHouseholdRepository,
    val sources: LocalSourceRegistry,
    val continueWatching: LocalFirstContinueWatchingRepository,
    val profileLibrary: LocalFirstProfileLibraryRepository,
) {
    /**
     * Removes a profile and its source assignments. Removing the household
     * record first keeps a failed follow-up cleanup from exposing a source to
     * another profile; retrying this command completes the idempotent cleanup.
     */
    suspend fun removeProfile(id: HouseholdProfileId) {
        household.removeProfile(id)
        sources.removeProfileAccess(id)
        continueWatching.clear(id)
        profileLibrary.removeProfile(id)
    }
}

suspend fun openLocalApplicationState(
    documents: LocalDocumentStore,
    sourceSecrets: LocalSourceSecretStore,
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
    profileLibraryOptions: ProfileLibraryOptions = ProfileLibraryOptions(),
): LocalApplicationState = LocalApplicationState(
    household = LocalFirstHouseholdRepository(
        PersistentHouseholdStore.open(documents),
        syncSources.household,
    ),
    sources = LocalSourceRegistry(
        PersistentLocalSourceStore.open(documents),
        sourceSecrets,
        syncSources.sourceMetadata,
    ),
    continueWatching = LocalFirstContinueWatchingRepository(
        PersistentContinueWatchingStore.open(documents),
        continueWatchingOptions,
        syncSources.continueWatching,
    ),
    profileLibrary = LocalFirstProfileLibraryRepository(
        PersistentProfileLibraryStore.open(documents),
        profileLibraryOptions,
        syncSources.profileLibrary,
    ),
)
