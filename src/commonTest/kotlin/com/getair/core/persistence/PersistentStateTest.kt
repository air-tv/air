package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingState
import com.getair.core.history.OnDemandContentRef
import com.getair.core.history.WatchProgress
import com.getair.core.household.HouseholdProfile
import com.getair.core.household.HouseholdProfileId
import com.getair.core.household.HouseholdState
import com.getair.core.source.LocalSourceId
import com.getair.core.source.LocalSourceKind
import com.getair.core.source.LocalSourceProfile
import com.getair.core.source.LocalSourceState
import com.getair.core.source.LocalSourceScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PersistentStateTest {
    @Test
    fun typedApplicationStateSurvivesReopening() = runTest {
        val documents = InMemoryDocumentStore()
        val profileId = HouseholdProfileId("alex")
        val household = HouseholdState(
            profiles = listOf(HouseholdProfile(profileId, "Alex", "AX")),
            selectedProfileId = profileId,
            revision = 2,
        )
        val sources = LocalSourceState(
            profiles = listOf(LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.Xtream)),
            revision = 3,
        )
        val history = ContinueWatchingState(
            entriesByProfile = mapOf(
                profileId to listOf(
                    WatchProgress(OnDemandContentRef.Stremio("movie", "tt-test"), 5_000, 10_000, 42),
                ),
            ),
            revision = 4,
        )

        PersistentHouseholdStore.open(documents).replace(household)
        PersistentLocalSourceStore.open(documents).replace(sources)
        PersistentContinueWatchingStore.open(documents).replace(history)

        assertEquals(household, PersistentHouseholdStore.open(documents).state.value)
        assertEquals(sources, PersistentLocalSourceStore.open(documents).state.value)
        assertEquals(history, PersistentContinueWatchingStore.open(documents).state.value)
        assertFalse("provider" in documents.toString())
    }

    @Test
    fun failedDurableWriteDoesNotPublishUnsavedState() = runTest {
        val documents = object : LocalDocumentStore {
            override suspend fun read(document: String): String? = null
            override suspend fun write(document: String, value: String) {
                throw IllegalStateException("disk unavailable")
            }
            override suspend fun remove(document: String) = Unit
        }
        val store = PersistentHouseholdStore.open(documents)
        val profileId = HouseholdProfileId("alex")
        val next = HouseholdState(
            profiles = listOf(HouseholdProfile(profileId, "Alex", "AX")),
            selectedProfileId = profileId,
        )

        assertFailsWith<IllegalStateException> { store.replace(next) }
        assertEquals(HouseholdState(), store.state.value)
    }

    @Test
    fun corruptDocumentFailureDoesNotEchoItsContents() = runTest {
        val secret = "do-not-print-this-document"
        val documents = object : LocalDocumentStore {
            override suspend fun read(document: String): String = "{\"password\":\"$secret\"}"
            override suspend fun write(document: String, value: String) = Unit
            override suspend fun remove(document: String) = Unit
        }

        val error = assertFailsWith<IllegalStateException> {
            PersistentHouseholdStore.open(documents)
        }

        assertFalse(secret in error.toString())
        assertEquals(null, error.cause)
    }

    @Test
    fun browserDocumentBehaviorNamespacesAndBoundsNonSecretState() = runTest {
        val values = mutableMapOf<String, String>()
        val store = BrowserDocumentStore(
            namespace = "air-test",
            readValue = values::get,
            writeValue = values::set,
            removeValue = values::remove,
            maximumDocumentChars = 8,
        )

        store.write("profile.v1", "state")
        assertEquals("state", values["air-test:profile.v1"])
        assertEquals("state", store.read("profile.v1"))
        assertFailsWith<IllegalArgumentException> { store.write("large.v1", "123456789") }
        store.remove("profile.v1")
        assertEquals(null, store.read("profile.v1"))
    }

    @Test
    fun sourceScopesAreVersionedMetadataAndNeverContainCredentials() = runTest {
        var encoded: String? = null
        val documents = object : LocalDocumentStore {
            override suspend fun read(document: String): String? = encoded
            override suspend fun write(document: String, value: String) {
                encoded = value
            }
            override suspend fun remove(document: String) {
                encoded = null
            }
        }
        val alex = HouseholdProfileId("alex")
        val state = LocalSourceState(
            profiles = listOf(
                LocalSourceProfile(
                    LocalSourceId("alex-addon"),
                    "Alex addon",
                    LocalSourceKind.StremioAddon,
                    scope = LocalSourceScope.selectedProfiles(listOf(alex)),
                ),
            ),
            revision = 7,
        )

        PersistentLocalSourceStore.open(documents).replace(state)

        val document = requireNotNull(encoded)
        assertTrue("\"version\":1" in document)
        assertTrue("alex-addon" in document)
        assertTrue("alex" in document)
        assertFalse("manifestUrl" in document)
        assertFalse("password" in document)
        assertEquals(state, PersistentLocalSourceStore.open(documents).state.value)
    }

    @Test
    fun sourceMetadataFromBeforeScopesDefaultsToGlobalAccess() = runTest {
        val legacy = """
            {"version":1,"value":{"profiles":[{"id":"legacy","name":"Legacy","kind":"M3u","enabled":true}],"revision":1}}
        """.trimIndent()
        val documents = object : LocalDocumentStore {
            override suspend fun read(document: String): String = legacy
            override suspend fun write(document: String, value: String) = Unit
            override suspend fun remove(document: String) = Unit
        }

        val restored = PersistentLocalSourceStore.open(documents).state.value

        assertEquals(LocalSourceScope.Global, restored.profiles.single().scope)
        assertEquals(
            listOf(LocalSourceId("legacy")),
            restored.sourcesFor(HouseholdProfileId("any-profile")).map { it.id },
        )
    }
}
