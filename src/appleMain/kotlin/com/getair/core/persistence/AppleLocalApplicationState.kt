package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.source.AppleKeychainLocalSourceSecretStore

suspend fun openAppleLocalApplicationState(
    applicationSupportDirectory: String,
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
): LocalApplicationState = openLocalApplicationState(
    documents = AppleFileDocumentStore(applicationSupportDirectory),
    sourceSecrets = AppleKeychainLocalSourceSecretStore(),
    syncSources = syncSources,
    continueWatchingOptions = continueWatchingOptions,
)
