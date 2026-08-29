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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

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
    private val profileCommands = Mutex()

    /**
     * Removes profile-owned state before removing the household record. The
     * record remains a durable retry handle until every idempotent cleanup has
     * completed, including across application restarts. Source pruning only
     * removes this profile from an existing restricted scope and never widens
     * that scope to global access.
    */
    suspend fun removeProfile(id: HouseholdProfileId) = profileCommands.withLock {
        removalStep({ LocalProfileRemovalException.SourceAccessRemoval() }) { sources.removeProfileAccess(id) }
        removalStep({ LocalProfileRemovalException.ContinueWatchingRemoval() }) { continueWatching.clear(id) }
        removalStep({ LocalProfileRemovalException.ProfileLibraryRemoval() }) { profileLibrary.removeProfile(id) }
        removalStep({ LocalProfileRemovalException.HouseholdRecordRemoval() }) { household.removeProfile(id) }
    }
}

sealed class LocalProfileRemovalException(message: String) : IllegalStateException(message) {
    class SourceAccessRemoval : LocalProfileRemovalException("Profile source access could not be removed")
    class ContinueWatchingRemoval : LocalProfileRemovalException("Profile watch history could not be removed")
    class ProfileLibraryRemoval : LocalProfileRemovalException("Profile library could not be removed")
    class HouseholdRecordRemoval : LocalProfileRemovalException("Household profile could not be removed")
}

private suspend fun removalStep(
    failure: () -> LocalProfileRemovalException,
    action: suspend () -> Unit,
) {
    try {
        action()
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        throw failure()
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
