package com.getair.core.catalog

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.await
import kotlin.js.Promise
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContains
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

    @Test
    fun durableGuideContractRunsInARealBrowser() = runTest(timeout = 60.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbGuideContract(uniqueDatabase("guide")) { databaseName, nowMillis ->
            openBrowserDurableCatalogStoreForTest(databaseName, nowMillis = nowMillis)
        }
    }

    @Test
    fun durableGuideIsSharedAcrossBrowserConnectionsWithoutPersistingSensitiveFields() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbGuideCrossInstance(uniqueDatabase("guide-shared")) { databaseName, nowMillis ->
            openBrowserDurableCatalogStoreForTest(databaseName, nowMillis = nowMillis)
        }
    }

    @Test
    fun startupCleanupRemovesAbandonedGuideRows() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbGuideStartupCleanup(
            uniqueDatabase("guide-startup-cleanup"),
            openWithoutGuideCleanup = { databaseName, nowMillis ->
                openBrowserDurableCatalogStoreForTest(databaseName, nowMillis = nowMillis)
            },
            openNormally = ::openBrowserDurableCatalogStore,
        )
    }

    @Test
    fun versionThreeUpgradePreservesVersionTwoMediaStores() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("v2-upgrade")
        createLegacyV2Database(databaseName).await()
        openBrowserDurableCatalogStore(databaseName).close()
        val inspection = inspectUpgradedDatabase(databaseName).await()
        assertContains(inspection, "\"version\":3")
        assertContains(inspection, "\"legacy\":true")
        listOf("guideStates", "guideGenerations", "guideChannels", "guideProgrammes", "guideLeases", "guideCleanupQueue")
            .forEach { store -> assertContains(inspection, "\"$store\"") }
    }

    @Test
    fun liveRuntimeConnectionClosesForARequestedVersionChange() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("version-change")
        val catalog = openBrowserDurableCatalogStore(databaseName)
        assertContains(upgradeDatabaseToVersionFour(databaseName).await(), "\"version\":4")
        catalog.close()
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

@Suppress("UNUSED_PARAMETER")
private fun createLegacyV2Database(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName, 2);
      request.onerror = function() { reject(request.error); };
      request.onblocked = function() { reject(new Error('AIR_IDB_BLOCKED')); };
      request.onupgradeneeded = function() {
        var database = request.result;
        database.createObjectStore('sources');
        database.createObjectStore('generations');
        database.createObjectStore('orphanQueue');
        database.createObjectStore('catalogRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
        database.createObjectStore('channelRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
        database.createObjectStore('programmes');
        database.createObjectStore('counters');
      };
      request.onsuccess = function() {
        var database = request.result;
        var transaction = database.transaction(['sources'], 'readwrite');
        transaction.objectStore('sources').put(JSON.stringify({ legacy: true }), 'legacy-source');
        transaction.oncomplete = function() { database.close(); resolve('ok'); };
        transaction.onerror = function() { reject(transaction.error); };
      };
    })
""")

@Suppress("UNUSED_PARAMETER")
private fun inspectUpgradedDatabase(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName);
      request.onerror = function() { reject(request.error); };
      request.onsuccess = function() {
        var database = request.result;
        var version = database.version;
        var stores = Array.from(database.objectStoreNames);
        var transaction = database.transaction(['sources'], 'readonly');
        var get = transaction.objectStore('sources').get('legacy-source');
        get.onerror = function() { reject(get.error); };
        get.onsuccess = function() {
          database.close();
          resolve(JSON.stringify({ version: version, stores: stores, legacy: JSON.parse(get.result).legacy }));
        };
      };
    })
""")

@Suppress("UNUSED_PARAMETER")
private fun upgradeDatabaseToVersionFour(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName, 4);
      request.onerror = function() { reject(request.error); };
      request.onblocked = function() { reject(new Error('AIR_IDB_BLOCKED')); };
      request.onsuccess = function() {
        var database = request.result;
        resolve(JSON.stringify({ version: database.version }));
        database.close();
      };
    })
""")

private fun indexedDbAvailable(): Boolean = js("typeof indexedDB !== 'undefined'")
