package com.getair.core.persistence

import android.content.Context
import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.source.AndroidKeystoreLocalSourceSecretStore

suspend fun openAndroidLocalApplicationState(
    context: Context,
    documentDirectoryName: String = "air-state",
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
): LocalApplicationState = openLocalApplicationState(
    documents = AndroidFileDocumentStore(context, documentDirectoryName),
    sourceSecrets = AndroidKeystoreLocalSourceSecretStore(context),
    syncSources = syncSources,
    continueWatchingOptions = continueWatchingOptions,
)
