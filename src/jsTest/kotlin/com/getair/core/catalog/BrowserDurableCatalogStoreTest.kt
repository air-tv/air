package com.getair.core.catalog

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.await
import kotlin.js.Promise
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds

class BrowserDurableCatalogStoreTest {
    @Test
    fun m3uMetadataContractRunsInARealBrowser() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("m3u")
        verifyDurableM3uCatalogContract { openBrowserDurableCatalogStore(databaseName) }
    }

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
    fun versionFourUpgradePreservesVersionTwoMediaStores() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("v2-upgrade")
        createLegacyV2Database(databaseName).await()
        openBrowserDurableCatalogStore(databaseName).close()
        val inspection = inspectUpgradedDatabase(databaseName).await()
        assertContains(inspection, "\"version\":4")
        assertContains(inspection, "\"legacy\":true")
        listOf("guideStates", "guideGenerations", "guideChannels", "guideProgrammes", "guideTimeline", "guideMigration", "guideLeases", "guideCleanupQueue")
            .forEach { store -> assertContains(inspection, "\"$store\"") }
    }

    @Test
    fun versionFourBackfillsPriorVersionThreeGuideIndexesAndBounds() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("v3-guide-upgrade")
        createLegacyV3GuideDatabase(databaseName).await()
        val catalog = openBrowserDurableCatalogStore(databaseName)
        assertEquals(2, (catalog as IndexedDbMigrationTestAccess).awaitGuideMigrationForTest())
        catalog.close()
        val complete = inspectLegacyV3GuideUpgrade(databaseName).await()
        assertContains(complete, "\"version\":4")
        assertContains(complete, "\"legacy\":true")
        assertContains(complete, "\"timelineCount\":602")
        assertContains(complete, "\"complete\":true")
        assertContains(complete, "\"maxFiniteSpanMs\":1000")
        assertContains(complete, "\"minStartMs\":500")
        assertContains(complete, "\"finiteStartMs\":500")
        assertContains(complete, "\"openStartMs\":800")
        assertContains(complete, "\"generationChannelFiniteStart\"")
        assertContains(complete, "\"generationChannelOpenStart\"")
    }

    @Test
    fun liveRuntimeConnectionClosesForARequestedVersionChange() = runTest(timeout = 30.seconds) {
        if (!verifyIndexedDbIsAvailableOrFailsHonestly()) return@runTest
        val databaseName = uniqueDatabase("version-change")
        val catalog = openBrowserDurableCatalogStore(databaseName)
        assertContains(upgradeDatabaseToVersionFour(databaseName).await(), "\"version\":5")
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
private fun createLegacyV3GuideDatabase(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName, 3);
      request.onerror = function() { reject(request.error); };
      request.onupgradeneeded = function() {
        var database = request.result;
        database.createObjectStore('sources');
        database.createObjectStore('generations');
        database.createObjectStore('orphanQueue');
        database.createObjectStore('catalogRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
        database.createObjectStore('channelRecords', { keyPath: 'recordKey' }).createIndex('orderKey', 'orderKey', { unique: true });
        database.createObjectStore('programmes');
        database.createObjectStore('counters');
        database.createObjectStore('guideStates', { keyPath: 'key' }).createIndex('activeFeedKey', 'activeFeedKey', { unique: true });
        var generations = database.createObjectStore('guideGenerations', { keyPath: 'key' });
        generations.createIndex('sourceEpochKey', 'sourceEpochKey', { unique: false });
        generations.createIndex('sourceFeedGeneration', ['sourceKey', 'feedId', 'generation'], { unique: true });
        database.createObjectStore('guideChannels', { keyPath: 'key' }).createIndex('generationChannel', ['generationKey', 'channelKey'], { unique: true });
        var programmes = database.createObjectStore('guideProgrammes', { keyPath: 'key' });
        programmes.createIndex('endKey', 'endKey', { unique: true });
        programmes.createIndex('generationChannelStart', ['generationKey', 'channelKey', 'startMs'], { unique: true });
        programmes.createIndex('generationChannelEffectiveEnd', ['generationKey', 'channelKey', 'effectiveEndMs', 'startMs'], { unique: true });
        programmes.createIndex('generationLocator', ['generationKey', 'key'], { unique: true });
        var leases = database.createObjectStore('guideLeases', { keyPath: 'key' });
        leases.createIndex('generationKey', 'generationKey', { unique: false });
        leases.createIndex('expiresAt', 'expiresAt', { unique: false });
        database.createObjectStore('guideCleanupQueue', { keyPath: 'key' }).createIndex('cleanupAt', 'cleanupAt', { unique: false });
      };
      request.onsuccess = function() {
        var database = request.result;
        var transaction = database.transaction(['sources', 'guideGenerations', 'guideProgrammes'], 'readwrite');
        transaction.objectStore('sources').put(JSON.stringify({ legacy: true }), 'legacy-source');
        transaction.objectStore('guideGenerations').put({
          key: 'legacy-generation', sourceKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          sourceStateKey: 'GS|aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', feedId: 'prior-v3',
          sourceEpoch: 1, sourceEpochKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|0000000000000001',
          feedStateKey: 'legacy-feed', generation: 1, mutationEpoch: 1,
          retention: { anchorMs: 1000, retainedFromMs: 1000, retainedUntilMs: 10000 },
          channelPrefix: 'legacy-channel|', programmePrefix: 'legacy-programme|', status: 'active', expiresAt: 0,
          batchCount: 3, inputChannelRows: 0, inputProgrammeRows: 602, channelCount: 0, programmeCount: 602,
          cleanupStarted: false
        });
        var store = transaction.objectStore('guideProgrammes');
        store.put({ key: 'legacy-finite', endKey: 'legacy-end-finite', generationKey: 'legacy-generation',
          channelKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
          winnerKey: 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
          startMs: 500, endMs: 1500, effectiveEndMs: 1500, title: 'finite', subtitle: null,
          description: null, categories: [], episode: null, artworkReference: null });
        store.put({ key: 'legacy-open', endKey: 'legacy-end-open', generationKey: 'legacy-generation',
          channelKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
          winnerKey: 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
          startMs: 800, endMs: null, effectiveEndMs: 3093527980800000, title: 'open', subtitle: null,
          description: null, categories: [], episode: null, artworkReference: null });
        for (var index = 0; index < 600; index += 1) {
          var start = 2000 + index;
          var suffix = String(index).padStart(4, '0');
          store.put({ key: 'legacy-row-' + suffix, endKey: 'legacy-end-row-' + suffix,
            generationKey: 'legacy-generation',
            channelKey: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            winnerKey: 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
            startMs: start, endMs: start + 1000, effectiveEndMs: start + 1000, title: 'row-' + suffix,
            subtitle: null, description: null, categories: [], episode: null, artworkReference: null });
        }
        transaction.oncomplete = function() { database.close(); resolve('ok'); };
        transaction.onerror = function() { reject(transaction.error); };
      };
    })
""")

@Suppress("UNUSED_PARAMETER")
private fun inspectLegacyV3GuideUpgrade(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName);
      request.onerror = function() { reject(request.error); };
      request.onsuccess = function() {
        var database = request.result;
        var transaction = database.transaction(['sources', 'guideGenerations', 'guideTimeline', 'guideMigration'], 'readonly');
        var legacy = transaction.objectStore('sources').get('legacy-source');
        var generation = transaction.objectStore('guideGenerations').get('legacy-generation');
        var timeline = transaction.objectStore('guideTimeline');
        var timelineCount = timeline.count();
        var finite = timeline.get('legacy-finite');
        var open = timeline.get('legacy-open');
        var migration = transaction.objectStore('guideMigration').get('legacy-v3');
        transaction.oncomplete = function() {
          var result = JSON.stringify({ version: database.version, legacy: JSON.parse(legacy.result).legacy,
            maxFiniteSpanMs: generation.result.maxFiniteSpanMs, minStartMs: generation.result.minStartMs,
            timelineCount: timelineCount.result, complete: migration.result.complete,
            finiteStartMs: finite.result && finite.result.finiteStartMs,
            openStartMs: open.result && open.result.openStartMs,
            indexes: Array.from(database.transaction(['guideTimeline'], 'readonly').objectStore('guideTimeline').indexNames) });
          database.close(); resolve(result);
        };
        transaction.onerror = function() { reject(transaction.error); };
      };
    })
""")

@Suppress("UNUSED_PARAMETER")
private fun upgradeDatabaseToVersionFour(databaseName: String): Promise<String> = js("""
    new Promise(function(resolve, reject) {
      var request = indexedDB.open(databaseName, 5);
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
