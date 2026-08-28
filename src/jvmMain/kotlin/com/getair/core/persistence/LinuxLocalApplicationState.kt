package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingOptions
import com.getair.core.source.KWalletLocalSourceSecretStore
import java.nio.file.Path

suspend fun openLinuxLocalApplicationState(
    documentDirectory: Path,
    wallet: String = "kdewallet",
    walletFolder: String = "Air TV",
    syncSources: LocalApplicationSyncSources = LocalApplicationSyncSources.None,
    continueWatchingOptions: ContinueWatchingOptions = ContinueWatchingOptions(),
): LocalApplicationState = openLocalApplicationState(
    documents = JvmFileDocumentStore(documentDirectory),
    sourceSecrets = KWalletLocalSourceSecretStore(wallet = wallet, folder = walletFolder),
    syncSources = syncSources,
    continueWatchingOptions = continueWatchingOptions,
)
