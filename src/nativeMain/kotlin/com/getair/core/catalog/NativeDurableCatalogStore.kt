package com.getair.core.catalog

import app.cash.sqldelight.driver.native.NativeSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext

/**
 * Opens a Native catalog. Apple and desktop shells provide a private absolute
 * database path or a platform-private database name.
 */
@OptIn(ExperimentalCoroutinesApi::class)
suspend fun openNativeDurableCatalogStore(
    databaseName: String,
    options: CatalogStorageOptions = CatalogStorageOptions(),
): DurableCatalogStore {
    require(databaseName.isNotBlank() && '\u0000' !in databaseName) {
        "Catalog database name is invalid"
    }
    val dispatcher = Dispatchers.Default.limitedParallelism(1)
    val store = withContext(dispatcher) {
        val driver = NativeSqliteDriver(AirCatalogDatabase.Schema, databaseName)
        // SQLite's rollback journal and FULL synchronous mode are the defaults.
        // SQLiter classifies PRAGMA statements as queries, including assignment
        // forms, so do not route redundant setup PRAGMAs through `execute`.
        // The store already serializes all writes through one dispatcher.
        SqlDelightDurableCatalogStore(driver, dispatcher)
    }
    store.cleanupUnreachable(options.startupCleanupRows)
    return store
}
