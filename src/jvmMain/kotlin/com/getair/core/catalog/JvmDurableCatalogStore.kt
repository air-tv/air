package com.getair.core.catalog

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.getair.core.catalog.db.AirCatalogDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

/** Opens the JVM desktop catalog without enabling WAL. */
@OptIn(ExperimentalCoroutinesApi::class)
suspend fun openJvmDurableCatalogStore(
    databasePath: Path,
    options: CatalogStorageOptions = CatalogStorageOptions(),
): DurableCatalogStore {
    val dispatcher = Dispatchers.IO.limitedParallelism(1)
    val store = withContext(dispatcher) {
        val absolute = databasePath.toAbsolutePath().normalize()
        absolute.parent?.let(Files::createDirectories)
        val driver = JdbcSqliteDriver(
            url = "jdbc:sqlite:$absolute",
            properties = Properties(),
            schema = AirCatalogDatabase.Schema,
        )
        driver.execute(null, "PRAGMA journal_mode=DELETE", 0)
        driver.execute(null, "PRAGMA synchronous=FULL", 0)
        driver.execute(null, "PRAGMA busy_timeout=5000", 0)
        SqlDelightDurableCatalogStore(driver, dispatcher)
    }
    store.cleanupUnreachable(options.startupCleanupRows)
    store.guides.cleanupUnreachable(options.startupCleanupRows)
    return store
}
