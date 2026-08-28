package com.getair.core.catalog

import android.content.Context
import app.cash.sqldelight.driver.android.AndroidSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext

/** Opens the Android catalog in the app's private database directory. */
@OptIn(ExperimentalCoroutinesApi::class)
suspend fun openAndroidDurableCatalogStore(
    context: Context,
    databaseName: String = "air-catalog.db",
    options: CatalogStorageOptions = CatalogStorageOptions(),
): DurableCatalogStore {
    require(databaseName.isNotBlank() && '/' !in databaseName && '\\' !in databaseName) {
        "Catalog database name is invalid"
    }
    val dispatcher = Dispatchers.IO.limitedParallelism(1)
    val store = withContext(dispatcher) {
        val driver = AndroidSqliteDriver(AirCatalogDatabase.Schema, context, databaseName)
        driver.execute(null, "PRAGMA journal_mode=DELETE", 0)
        driver.execute(null, "PRAGMA synchronous=FULL", 0)
        driver.execute(null, "PRAGMA busy_timeout=5000", 0)
        SqlDelightDurableCatalogStore(driver, dispatcher)
    }
    store.cleanupUnreachable(options.startupCleanupRows)
    store.guides.cleanupUnreachable(options.startupCleanupRows)
    return store
}
