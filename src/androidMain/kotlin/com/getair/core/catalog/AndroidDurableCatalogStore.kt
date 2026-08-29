package com.getair.core.catalog

import android.content.Context
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
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
        try {
            driver.executePragma("PRAGMA journal_mode=DELETE")
            driver.executePragma("PRAGMA synchronous=FULL")
            driver.executePragma("PRAGMA busy_timeout=5000")
            SqlDelightDurableCatalogStore(driver, dispatcher)
        } catch (failure: Throwable) {
            driver.close()
            throw failure
        }
    }
    store.cleanupUnreachable(options.startupCleanupRows)
    store.guides.cleanupUnreachable(options.startupCleanupRows)
    return store
}

/** Android's execSQL path rejects PRAGMAs that return a row, including journal_mode. */
private fun SqlDriver.executePragma(sql: String) {
    executeQuery(
        identifier = null,
        sql = sql,
        mapper = { cursor ->
            while (cursor.next().value) Unit
            QueryResult.Value(Unit)
        },
        parameters = 0,
        binders = null,
    ).value
}
