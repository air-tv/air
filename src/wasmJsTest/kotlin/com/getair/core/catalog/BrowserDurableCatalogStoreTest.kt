package com.getair.core.catalog

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.await
import kotlin.js.JsString
import kotlin.js.Promise
import kotlin.js.toJsString
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
    fun durableGuideReviewRegressionsRunInARealBrowser() = runTest(timeout = 60.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        verifyIndexedDbGuideReviewRegressions(uniqueDatabase("guide-review")) { databaseName, nowMillis ->
            openBrowserDurableCatalogStoreForTest(databaseName, nowMillis = nowMillis)
        }
    }

    @Test
    fun versionThreeUpgradePreservesVersionTwoMediaStores() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("v2-upgrade")
        createLegacyV2Database(databaseName.toJsString()).await<JsString>()
        openBrowserDurableCatalogStore(databaseName).close()
        val inspection = inspectUpgradedDatabase(databaseName.toJsString()).await<JsString>().toString()
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
        val upgraded = upgradeDatabaseToVersionFour(databaseName.toJsString()).await<JsString>().toString()
        assertContains(upgraded, "\"version\":4")
        catalog.close()
    }

    private fun uniqueDatabase(suffix: String): String =
        "air-wasm-$suffix-${Random.nextInt().toUInt()}"

    private suspend fun verifyIndexedDbIsAvailableOrFailsHonestly(): Boolean {
        if (indexedDbAvailable()) return true
        assertFailsWith<CatalogStoreException> {
            openBrowserDurableCatalogStore(uniqueDatabase("unavailable"))
        }
        return false
    }
}

@JsFun("""(databaseName) => new Promise((resolve, reject) => {
  const request = indexedDB.open(databaseName, 2);
  request.onerror = () => reject(request.error);
  request.onblocked = () => reject(new Error('AIR_IDB_BLOCKED'));
  request.onupgradeneeded = () => {
    const database = request.result;
    database.createObjectStore('sources');
    database.createObjectStore('generations');
    database.createObjectStore('orphanQueue');
    database.createObjectStore('catalogRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
    database.createObjectStore('channelRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
    database.createObjectStore('programmes');
    database.createObjectStore('counters');
  };
  request.onsuccess = () => {
    const database = request.result;
    const transaction = database.transaction(['sources'], 'readwrite');
    transaction.objectStore('sources').put(JSON.stringify({ legacy: true }), 'legacy-source');
    transaction.oncomplete = () => { database.close(); resolve('ok'); };
    transaction.onerror = () => reject(transaction.error);
  };
})""")
private external fun createLegacyV2Database(databaseName: JsString): Promise<JsString>

@JsFun("""(databaseName) => new Promise((resolve, reject) => {
  const request = indexedDB.open(databaseName);
  request.onerror = () => reject(request.error);
  request.onsuccess = () => {
    const database = request.result;
    const version = database.version;
    const stores = Array.from(database.objectStoreNames);
    const transaction = database.transaction(['sources'], 'readonly');
    const get = transaction.objectStore('sources').get('legacy-source');
    get.onerror = () => reject(get.error);
    get.onsuccess = () => {
      database.close();
      resolve(JSON.stringify({ version, stores, legacy: JSON.parse(get.result).legacy }));
    };
  };
})""")
private external fun inspectUpgradedDatabase(databaseName: JsString): Promise<JsString>

@JsFun("""(databaseName) => new Promise((resolve, reject) => {
  const request = indexedDB.open(databaseName, 4);
  request.onerror = () => reject(request.error);
  request.onblocked = () => reject(new Error('AIR_IDB_BLOCKED'));
  request.onsuccess = () => {
    const database = request.result;
    resolve(JSON.stringify({ version: database.version }));
    database.close();
  };
})""")
private external fun upgradeDatabaseToVersionFour(databaseName: JsString): Promise<JsString>

@JsFun("() => typeof indexedDB !== 'undefined'")
private external fun indexedDbAvailable(): Boolean
