package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.history.ContinueWatchingSyncSource
import com.getair.core.history.LocalFirstContinueWatchingRepository
import com.getair.core.household.HouseholdSyncSource
import com.getair.core.household.LocalFirstHouseholdRepository
import com.getair.core.source.LocalSourceRegistry
import com.getair.core.source.LocalSourceSecretStore
import com.getair.core.source.SourceMetadataSyncSource

/** Optional future-server seams; ordinary local construction uses [None]. */
class LocalApplicationSyncSources(
    val household: HouseholdSyncSource? = null,
    val sourceMetadata: SourceMetadataSyncSource? = null,
    val continueWatching: ContinueWatchingSyncSource? = null,
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
)

suspend fun openLocalApplicationState(
    documents: LocalDocumentStore,
    sourceSecrets: LocalSourceSecretStore,
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
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
)
