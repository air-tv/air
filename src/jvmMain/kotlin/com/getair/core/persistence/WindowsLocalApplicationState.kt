package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.source.WindowsDpapiLocalSourceSecretStore
import java.nio.file.Path

suspend fun openWindowsLocalApplicationState(
    documentDirectory: Path,
    credentialDirectory: Path = documentDirectory.resolve("credentials"),
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
): LocalApplicationState = openLocalApplicationState(
    documents = JvmFileDocumentStore(documentDirectory.resolve("state")),
    sourceSecrets = WindowsDpapiLocalSourceSecretStore(credentialDirectory),
    syncSources = syncSources,
    continueWatchingOptions = continueWatchingOptions,
)
