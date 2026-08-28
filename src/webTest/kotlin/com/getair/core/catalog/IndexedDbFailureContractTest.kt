package com.getair.core.catalog

// Browser-only failure mapping tests; native/JVM do not compile the IndexedDB policy.

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class IndexedDbFailureContractTest {
    @Test
    fun unavailableAndQuotaFailuresRemainActionableAndRedacted() = runTest {
        val unavailable = assertFailsWith<CatalogStoreException> {
            openIndexedDbDurableCatalogStore("unavailable", FailingExecutor("AIR_IDB_UNAVAILABLE"))
        }
        assertTrue("unavailable" in unavailable.message.orEmpty().lowercase())

        val quota = assertFailsWith<CatalogStoreException> {
            openIndexedDbDurableCatalogStore("quota", FailingExecutor("AIR_IDB_QUOTA"))
        }
        assertTrue("quota" in quota.message.orEmpty().lowercase())
        assertTrue("provider-secret" !in quota.stackTraceToString())
    }

    @Test
    fun coroutineCancellationIsNeverWrappedAsAStorageFailure() = runTest {
        assertFailsWith<CancellationException> {
            openIndexedDbDurableCatalogStore("cancel", CancellingExecutor)
        }
    }

    private class FailingExecutor(private val code: String) : BrowserIndexedDbExecutor {
        override suspend fun execute(
            databaseName: String,
            operationId: String,
            commandJson: String,
        ): String = throw BrowserIndexedDbFailure(code)

        override fun close(databaseName: String) = Unit
    }

    private object CancellingExecutor : BrowserIndexedDbExecutor {
        override suspend fun execute(
            databaseName: String,
            operationId: String,
            commandJson: String,
        ): String = throw CancellationException("cancelled")

        override fun close(databaseName: String) = Unit
    }
}
