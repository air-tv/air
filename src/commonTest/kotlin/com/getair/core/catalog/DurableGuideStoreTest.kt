package com.getair.core.catalog

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DurableGuideStoreTest {
    @Test
    fun commonDurableGuideContract() = runTest {
        verifyDurableGuideStoreContract(
            object : DurableGuideStoreContractFactory {
                override suspend fun create(): DurableGuideStore = ContractInMemoryDurableGuideStore()

                override suspend fun reopen(store: DurableGuideStore): DurableGuideStore =
                    (store as ContractInMemoryDurableGuideStore).reopen()

                override suspend fun advanceTimeBy(store: DurableGuideStore, milliseconds: Long) {
                    (store as ContractInMemoryDurableGuideStore).advanceTimeBy(milliseconds)
                }
            },
        )
    }

    @Test
    fun unsupportedCapabilityIsExplicitAndSanitized() = runTest {
        assertFalse(UnsupportedDurableGuideStore.supported)
        val failure = assertFailsWith<DurableGuideStoreException.Unsupported> {
            UnsupportedDurableGuideStore.cleanupUnreachable()
        }
        assertTrue(failure.message.orEmpty().contains("unsupported"))
        assertFalse(failure.message.orEmpty().contains("source"))
    }

    @Test
    fun generationTotalCapsAreEnforcedWithoutAllocatingHugeFixtures() = runTest {
        val store = ContractInMemoryDurableGuideStore()
        val key = DurableGuideKey(
            DurableGuideSourceKey("a".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS)),
            DurableGuideFeedId("limits"),
        )
        val retention = DurableGuideRetention(
            kotlinx.datetime.Instant.fromEpochMilliseconds(1_000),
            kotlinx.datetime.Instant.fromEpochMilliseconds(0),
            kotlinx.datetime.Instant.fromEpochMilliseconds(2_000),
        )
        val channelKey = DurableGuideChannelKey("b".repeat(DurableGuideLimits.OPAQUE_DIGEST_CHARS))
        val channels = store.beginRefresh(key, retention)
        store.forceGenerationCountsForTest(channels, DurableGuideLimits.MAX_GENERATION_CHANNELS, 0)
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.stage(channels, channels = listOf(DurableGuideChannelRecord(channelKey, listOf("channel"))))
        }

        val programmes = store.beginRefresh(key, retention)
        store.forceGenerationCountsForTest(programmes, 1, DurableGuideLimits.MAX_GENERATION_PROGRAMMES)
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.stage(
                programmes,
                programmes = listOf(
                    DurableGuideProgrammeRecord(
                        channelKey,
                        kotlinx.datetime.Instant.fromEpochMilliseconds(0),
                        kotlinx.datetime.Instant.fromEpochMilliseconds(1_000),
                        "programme",
                    ),
                ),
            )
        }

        val duplicateChannel = DurableGuideChannelRecord(channelKey, listOf("duplicate"))
        val duplicateProgramme = DurableGuideProgrammeRecord(
            channelKey,
            kotlinx.datetime.Instant.fromEpochMilliseconds(0),
            kotlinx.datetime.Instant.fromEpochMilliseconds(1_000),
            "duplicate",
        )

        val batches = store.beginRefresh(key, retention)
        store.forceGenerationWorkForTest(
            batches,
            DurableGuideLimits.MAX_GENERATION_BATCHES - 1,
            0,
            0,
        )
        store.stage(batches)
        assertTrue(store.renewGeneration(batches))
        assertFailsWith<DurableGuideStoreException.Limit> { store.stage(batches) }

        val inputChannels = store.beginRefresh(key, retention)
        store.stage(inputChannels, channels = listOf(duplicateChannel))
        store.forceGenerationWorkForTest(
            inputChannels,
            1,
            DurableGuideLimits.MAX_INPUT_CHANNEL_ROWS - 1,
            0,
        )
        store.stage(inputChannels, channels = listOf(duplicateChannel))
        assertTrue(store.renewGeneration(inputChannels))
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.stage(inputChannels, channels = listOf(duplicateChannel))
        }

        val inputProgrammes = store.beginRefresh(key, retention)
        store.stage(inputProgrammes, programmes = listOf(duplicateProgramme))
        store.forceGenerationWorkForTest(
            inputProgrammes,
            1,
            0,
            DurableGuideLimits.MAX_INPUT_PROGRAMME_ROWS - 1,
        )
        store.stage(inputProgrammes, programmes = listOf(duplicateProgramme))
        assertTrue(store.renewGeneration(inputProgrammes))
        assertFailsWith<DurableGuideStoreException.Limit> {
            store.stage(inputProgrammes, programmes = listOf(duplicateProgramme))
        }
    }
}
