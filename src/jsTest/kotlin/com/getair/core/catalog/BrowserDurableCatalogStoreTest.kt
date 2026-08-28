package com.getair.core.catalog

import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds

class BrowserDurableCatalogStoreTest {
    @Test
    fun indexedDbContractRunsInARealBrowser() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbCatalogContract(
            open = ::openBrowserDurableCatalogStore,
            databaseName = uniqueDatabase("contract"),
        )
    }

    @Test
    fun unsafeReferencesAreRejectedBeforeIndexedDb() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbRejectsUnsafeMetadata(
            open = ::openBrowserDurableCatalogStore,
            databaseName = uniqueDatabase("unsafe"),
        )
    }

    @Test
    fun largeFixtureIsIngestedAndReadOnlyInBoundedPages() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbBoundedLargeFixture(
            open = ::openBrowserDurableCatalogStore,
            databaseName = uniqueDatabase("large"),
        )
    }

    private fun uniqueDatabase(suffix: String): String =
        "air-js-$suffix-${Random.nextInt().toUInt()}"

    private suspend fun verifyIndexedDbIsAvailableOrFailsHonestly(): Boolean {
        if (indexedDbAvailable()) return true
        assertFailsWith<CatalogStoreException> {
            openBrowserDurableCatalogStore(uniqueDatabase("unavailable"))
        }
        return false
    }
}

private fun indexedDbAvailable(): Boolean = js("typeof indexedDB !== 'undefined'")
