package com.getair.core.library

import com.getair.core.history.OnDemandContentRef
import com.getair.core.household.HouseholdProfile
import com.getair.core.household.HouseholdProfileId
import com.getair.core.household.LiveTvBuffer
import com.getair.core.persistence.InMemoryDocumentStore
import com.getair.core.persistence.LocalDocumentStore
import com.getair.core.persistence.openLocalApplicationState
import com.getair.core.source.InMemoryLocalSourceSecretStore
import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.ChannelId
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProfileLibraryTest {
    private val alex = HouseholdProfileId("alex")
    private val kids = HouseholdProfileId("kids")

    @Test
    fun favoritesAndLiveHistoryAreBoundedDeterministicAndProfileIsolated() = runTest {
        val repository = LocalFirstProfileLibraryRepository(
            InMemoryProfileLibraryStore(),
            ProfileLibraryOptions(
                maximumFavoritesPerProfile = 2,
                maximumLiveHistoryPerProfile = 2,
            ),
        )
        val channelA = channel("a")
        val channelB = channel("b")
        val channelC = channel("c")

        repository.addFavorite(alex, channelB, 10)
        repository.addFavorite(alex, channelA, 10)
        repository.addFavorite(alex, channelC, 20)
        repository.addFavorite(kids, channelB, 30)
        repository.recordLiveChannel(alex, channelB, 10)
        repository.recordLiveChannel(alex, channelA, 10)
        repository.recordLiveChannel(alex, channelC, 20)

        assertEquals(listOf(channelC, channelA), repository.profile(alex).favorites.map { it.item })
        assertEquals(listOf(channelC, channelA), repository.profile(alex).liveChannelHistory.map { it.channel })
        assertEquals(listOf(channelB), repository.profile(kids).favorites.map { it.item })
        assertTrue(repository.profile(kids).liveChannelHistory.isEmpty())
    }

    @Test
    fun playbackPreferencesResolveGlobalThenProfileThenItemAndNormalizeLanguages() = runTest {
        val repository = LocalFirstProfileLibraryRepository(InMemoryProfileLibraryStore())
        val item = channel("news")
        repository.setGlobalPlaybackDefaults(
            PlaybackPreferenceDefaults(
                preferredAudioLanguage = "EN-US",
                preferredSubtitleLanguage = "ES",
                subtitlesEnabled = false,
                liveTvBuffer = LiveTvBuffer.LowLatency,
            ),
        )
        repository.setProfilePlaybackDefaults(
            alex,
            PlaybackPreferenceOverrides(
                preferredSubtitleLanguage = "FR",
                subtitlesEnabled = true,
            ),
        )
        repository.setItemPlaybackPreferences(
            alex,
            item,
            PlaybackPreferenceOverrides(
                preferredAudioLanguage = "JA",
                liveTvBuffer = LiveTvBuffer.Stable,
            ),
        )

        assertEquals(
            PlaybackPreferenceDefaults("ja", "fr", true, LiveTvBuffer.Stable),
            repository.resolvedPlaybackPreferences(alex, item),
        )
        assertEquals(
            PlaybackPreferenceDefaults("en-us", "es", false, LiveTvBuffer.LowLatency),
            repository.resolvedPlaybackPreferences(kids, item),
        )
    }

    @Test
    fun repeatedCommandsAreDurableNoOps() = runTest {
        val documents = CapturingDocumentStore()
        val repository = LocalFirstProfileLibraryRepository(PersistentProfileLibraryStore.open(documents))
        val item = stremioMovie("tt123")

        repository.addFavorite(alex, item, 42)
        val revision = repository.state.value.revision
        repository.addFavorite(alex, item, 42)
        repository.removeFavorite(kids, item)
        repository.clearLiveChannelHistory(kids)

        assertEquals(1, documents.writeCount)
        assertEquals(revision, repository.state.value.revision)
    }

    @Test
    fun failedDocumentWriteNeverPublishesTheUnsavedState() = runTest {
        val documents = object : LocalDocumentStore {
            override suspend fun read(document: String): String? = null
            override suspend fun write(document: String, value: String) {
                throw IllegalStateException("disk unavailable")
            }
            override suspend fun remove(document: String) = Unit
        }
        val repository = LocalFirstProfileLibraryRepository(PersistentProfileLibraryStore.open(documents))

        assertFailsWith<IllegalStateException> {
            repository.addFavorite(alex, stremioMovie("tt-unsaved"), 1)
        }
        assertEquals(ProfileLibraryState(), repository.state.value)
    }

    @Test
    fun versionedDocumentReopensWithoutPlaybackOrArtworkPayloads() = runTest {
        val documents = CapturingDocumentStore()
        val first = LocalFirstProfileLibraryRepository(PersistentProfileLibraryStore.open(documents))
        val favorite = LibraryItemRef.OnDemand(
            content = OnDemandContentRef.IptvMovie(com.getair.iptv.model.MovieId("movie-9")),
            sourceId = LocalSourceId("provider-a"),
        )
        first.addFavorite(alex, favorite, 90)
        first.recordLiveChannel(alex, channel("channel-7"), 100)

        val encoded = requireNotNull(documents.value)
        assertTrue("\"version\":1" in encoded)
        assertFalse("streamUrl" in encoded)
        assertFalse("logoUrl" in encoded)
        assertFalse("headers" in encoded)
        assertFalse("password" in encoded)
        assertEquals(first.state.value, PersistentProfileLibraryStore.open(documents).state.value)
    }

    @Test
    fun legacyV0FavoritesMigrateAndUnknownVersionsAreRejected() = runTest {
        val legacy = """
            {"version":0,"value":{"favoritesByProfile":{"alex":[{"item":{"_type":"live_channel","sourceId":"tv","channelId":"7"},"addedAtEpochMillis":10}]},"revision":3}}
        """.trimIndent()
        val documents = CapturingDocumentStore(legacy)

        val restored = PersistentProfileLibraryStore.open(documents).state.value

        assertEquals(3, restored.revision)
        assertEquals(listOf(channel("7", "tv")), restored.profiles.getValue(alex).favorites.map { it.item })
        documents.value = """{"version":99,"value":{"token":"never-echo-this"}}"""
        val error = assertFailsWith<IllegalStateException> {
            PersistentProfileLibraryStore.open(documents)
        }
        assertFalse("never-echo-this" in error.toString())
        assertNull(error.cause)
    }

    @Test
    fun malformedDocumentsAndUrlLikeIdentifiersCannotLeakSecretsIntoStateOrErrors() = runTest {
        val secret = "https://provider.invalid/live/user/password/1.ts"
        assertFailsWith<IllegalArgumentException> {
            LibraryItemRef.LiveChannel(LocalSourceId("tv"), ChannelId(secret))
        }
        val documents = CapturingDocumentStore("""{"version":1,"password":"$secret"}""")
        val error = assertFailsWith<IllegalStateException> {
            PersistentProfileLibraryStore.open(documents)
        }
        assertFalse(secret in error.toString())
        assertNull(error.cause)
    }

    @Test
    fun profileDeletionCleansEveryProfileScopedLibraryAndHistoryDocument() = runTest {
        val documents = InMemoryDocumentStore()
        val application = openLocalApplicationState(documents, InMemoryLocalSourceSecretStore())
        val profile = HouseholdProfile(alex, "Alex", "AX")
        application.household.upsertProfile(profile)
        application.profileLibrary.addFavorite(alex, stremioMovie("tt-delete"), 1)
        application.profileLibrary.recordLiveChannel(alex, channel("delete"), 2)

        application.removeProfile(alex)
        val reopened = openLocalApplicationState(documents, InMemoryLocalSourceSecretStore())

        assertTrue(reopened.profileLibrary.profile(alex).isEmpty)
        val revision = reopened.profileLibrary.state.value.revision
        reopened.removeProfile(alex)
        assertEquals(revision, reopened.profileLibrary.state.value.revision)
    }

    @Test
    fun optionalSyncSeamIsLocalByDefaultAndPublishesBoundedStateAtomically() = runTest {
        val local = LocalFirstProfileLibraryRepository(InMemoryProfileLibraryStore())
        assertIs<ProfileLibraryRefreshResult.LocalOnly>(local.refresh())
        val synchronized = LocalFirstProfileLibraryRepository(
            InMemoryProfileLibraryStore(),
            ProfileLibraryOptions(maximumFavoritesPerProfile = 2),
            ProfileLibrarySyncSource { previous ->
                previous.copy(
                    profiles = mapOf(
                        alex to ProfileLibrary(
                            favorites = listOf(
                                FavoriteEntry(stremioMovie("c"), 3),
                                FavoriteEntry(stremioMovie("b"), 2),
                                FavoriteEntry(stremioMovie("a"), 1),
                            ),
                        ),
                    ),
                        revision = 500,
                )
            },
        )

        assertIs<ProfileLibraryRefreshResult.Updated>(synchronized.refresh())
        assertEquals(listOf("c", "b"), synchronized.profile(alex).favorites.map {
            ((it.item as LibraryItemRef.OnDemand).content as OnDemandContentRef.Stremio).id
        })
        assertEquals(1, synchronized.state.value.revision)
    }

    private fun channel(id: String, source: String = "tv"): LibraryItemRef.LiveChannel =
        LibraryItemRef.LiveChannel(LocalSourceId(source), ChannelId(id))

    private fun stremioMovie(id: String): LibraryItemRef.OnDemand =
        LibraryItemRef.OnDemand(OnDemandContentRef.Stremio("movie", id))

    private class CapturingDocumentStore(initial: String? = null) : LocalDocumentStore {
        var value: String? = initial
        var writeCount: Int = 0

        override suspend fun read(document: String): String? = value
        override suspend fun write(document: String, value: String) {
            this.value = value
            writeCount += 1
        }
        override suspend fun remove(document: String) {
            value = null
        }
    }
}
