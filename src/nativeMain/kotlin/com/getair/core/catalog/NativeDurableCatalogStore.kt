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
        driver.execute(null, "PRAGMA journal_mode=DELETE", 0)
        driver.execute(null, "PRAGMA synchronous=FULL", 0)
        driver.execute(null, "PRAGMA busy_timeout=5000", 0)
        SqlDelightDurableCatalogStore(driver, dispatcher)
    }
    store.cleanupUnreachable(options.startupCleanupRows)
    return store
}
