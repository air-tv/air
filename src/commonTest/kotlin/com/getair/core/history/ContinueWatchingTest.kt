package com.getair.core.history

import com.getair.core.household.HouseholdProfileId
import com.getair.iptv.model.EpisodeId
import com.getair.iptv.model.SeriesId
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ContinueWatchingTest {
    @Test
    fun progressIsProfileScopedOrderedAndReplacedByContentIdentity() = runTest {
        val repository = LocalFirstContinueWatchingRepository(InMemoryContinueWatchingStore())
        val alex = HouseholdProfileId("alex")
        val sam = HouseholdProfileId("sam")
        val movie = OnDemandContentRef.Stremio("movie", "tt-air")

        repository.record(alex, progress(movie, 1_000, updated = 10))
        repository.record(alex, progress(episode("episode-1"), 2_000, updated = 20))
        repository.record(alex, progress(movie, 3_000, updated = 30))
        repository.record(sam, progress(movie, 4_000, updated = 40))

        assertEquals(listOf(movie, episode("episode-1")), repository.entries(alex).map { it.content })
        assertEquals(3_000, repository.entries(alex).first().positionMillis)
        assertEquals(4_000, repository.entries(sam).single().positionMillis)
    }

    @Test
    fun completedItemsLeaveTheShelfAndHistoryStaysBounded() = runTest {
        val repository = LocalFirstContinueWatchingRepository(
            InMemoryContinueWatchingStore(),
            ContinueWatchingOptions(maximumEntriesPerProfile = 2, completionThreshold = 0.9f),
        )
        val profile = HouseholdProfileId("living")
        repository.record(profile, progress(OnDemandContentRef.Stremio("movie", "one"), 1_000, updated = 1))
        repository.record(profile, progress(OnDemandContentRef.Stremio("movie", "two"), 1_000, updated = 2))
        repository.record(profile, progress(OnDemandContentRef.Stremio("movie", "three"), 1_000, updated = 3))
        assertEquals(listOf("three", "two"), repository.entries(profile).map {
            (it.content as OnDemandContentRef.Stremio).id
        })

        repository.record(profile, progress(OnDemandContentRef.Stremio("movie", "three"), 9_000, updated = 4))
        assertEquals(listOf("two"), repository.entries(profile).map {
            (it.content as OnDemandContentRef.Stremio).id
        })
    }

    @Test
    fun noServerIsAValidState() = runTest {
        val repository = LocalFirstContinueWatchingRepository(InMemoryContinueWatchingStore())
        assertIs<ContinueWatchingRefreshResult.LocalOnly>(repository.refresh())
        assertTrue(repository.state.value.entriesByProfile.isEmpty())
    }
}

private fun progress(
    content: OnDemandContentRef,
    position: Long,
    updated: Long,
) = WatchProgress(content, position, durationMillis = 10_000, updatedAtEpochMillis = updated)

private fun episode(id: String) = OnDemandContentRef.IptvEpisode(
    id = EpisodeId(id),
    seriesId = SeriesId("series"),
    season = 1,
    episode = 1,
)
