package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.source.InMemoryLocalSourceSecretStore

/** Browser metadata is durable; provider secrets intentionally last for this session only. */
suspend fun openBrowserLocalApplicationState(
    namespace: String = "air",
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
): LocalApplicationState = openLocalApplicationState(
    documents = browserLocalDocumentStore(namespace),
    sourceSecrets = InMemoryLocalSourceSecretStore(),
    syncSources = syncSources,
    continueWatchingOptions = continueWatchingOptions,
)
