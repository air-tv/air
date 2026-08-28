package com.getair.core.catalog

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.await

suspend fun openBrowserDurableCatalogStore(
    databaseName: String = "air-catalog",
    options: CatalogStorageOptions = CatalogStorageOptions(),
): DurableCatalogStore = openIndexedDbDurableCatalogStore(
    databaseName = databaseName,
    executor = JsIndexedDbExecutor,
    options = options,
)

private object JsIndexedDbExecutor : BrowserIndexedDbExecutor {
    override suspend fun execute(
        databaseName: String,
        operationId: String,
        commandJson: String,
    ): String = try {
        executeIndexedDbCommandRaw(databaseName, commandJson).await()
    } catch (cancelled: CancellationException) {
        executeIndexedDbCommandRaw(databaseName, cancelCommand(operationId))
        throw cancelled
    } catch (failure: Throwable) {
        throw BrowserIndexedDbFailure(failure.indexedDbFailureCode(), failure)
    }

    override fun close(databaseName: String) {
        executeIndexedDbCommandRaw(databaseName, "{\"op\":\"close\"}")
    }
}

private fun cancelCommand(operationId: String): String =
    "{\"op\":\"cancel\",\"targetOperationId\":\"$operationId\"}"

private fun Throwable.indexedDbFailureCode(): String = INDEXED_DB_FAILURE_CODES
    .firstOrNull { it in message.orEmpty() }
    ?: "AIR_IDB_FAILURE"

private val INDEXED_DB_FAILURE_CODES = listOf(
    "AIR_IDB_UNAVAILABLE",
    "AIR_IDB_BLOCKED",
    "AIR_IDB_QUOTA",
    "AIR_IDB_CANCELLED",
    "AIR_IDB_ABORT",
    "AIR_IDB_STALE",
    "AIR_IDB_NOT_WRITABLE",
    "AIR_IDB_ACTIVE_IMMUTABLE",
    "AIR_IDB_COUNT_MISMATCH",
    "AIR_IDB_GENERATION_EXHAUSTED",
    "AIR_IDB_CORRUPT",
    "AIR_IDB_COMMAND",
)
