package com.getair.core.media

import com.getair.core.household.HouseholdProfileId
import com.getair.core.source.LocalSourceId
import com.getair.core.source.LocalSourceKind
import com.getair.core.source.LocalSourceProfile
import com.getair.core.source.LocalSourceScope
import com.getair.core.source.LocalSourceState
import com.getair.iptv.model.ChannelId
import com.getair.iptv.model.ChannelKind
import com.getair.iptv.model.IptvChannel
import com.getair.iptv.model.IptvSourceKind
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame

class MediaRepositoryTest {
    @Test
    fun replacesAndRemovesOneSourceWithoutRebuildingUnrelatedMedia() = runTest {
        val firstId = LocalSourceId("first")
        val secondId = LocalSourceId("second")
        val repository = LocalFirstMediaRepository(InMemoryMediaStore())
        repository.replaceSource(firstId, SourceMedia(liveChannels = listOf(channel("one"))))
        repository.replaceSource(secondId, SourceMedia(liveChannels = listOf(channel("two"))))
        val retained = repository.snapshot.value.source(firstId)

        repository.replaceSource(secondId, SourceMedia(liveChannels = listOf(channel("updated"))))

        assertSame(retained, repository.snapshot.value.source(firstId))
        assertEquals(listOf("one"), repository.snapshot.value.source(firstId)?.liveChannels?.map { it.name })
        assertEquals(listOf("updated"), repository.snapshot.value.source(secondId)?.liveChannels?.map { it.name })
        assertEquals(3, repository.snapshot.value.revision)

        repository.removeSource(secondId)
        assertSame(retained, repository.snapshot.value.source(firstId))
        assertEquals(setOf(firstId), repository.snapshot.value.sources.keys)
        assertEquals(4, repository.snapshot.value.revision)
    }

    @Test
    fun profileReadsFollowSourceVisibilityAndSourceOrder() = runTest {
        val alex = HouseholdProfileId("alex")
        val kids = HouseholdProfileId("kids")
        val global = LocalSourceId("global")
        val alexOnly = LocalSourceId("alex-only")
        val disabled = LocalSourceId("disabled")
        val unconfigured = LocalSourceId("not-indexed")
        val sourceState = LocalSourceState(
            profiles = listOf(
                source(alexOnly, LocalSourceScope.selectedProfiles(listOf(alex))),
                source(global),
                source(disabled, enabled = false),
                source(unconfigured),
            ),
            revision = 7,
        )
        val repository = LocalFirstMediaRepository(InMemoryMediaStore())
        repository.replaceSource(global, SourceMedia(liveChannels = listOf(channel("global"))))
        repository.replaceSource(alexOnly, SourceMedia(liveChannels = listOf(channel("alex"))))
        repository.replaceSource(disabled, SourceMedia(liveChannels = listOf(channel("disabled"))))

        val alexMedia = repository.snapshot.value.visibleTo(alex, sourceState)
        val kidsMedia = repository.snapshot.value.visibleTo(kids, sourceState)

        assertEquals(listOf(alexOnly, global), alexMedia.sources.map(SourceMediaEntry::sourceId))
        assertEquals(listOf("alex", "global"), alexMedia.mergedLiveChannels().map { it.name })
        assertEquals(listOf(global), kidsMedia.sources.map(SourceMediaEntry::sourceId))
        assertEquals(7, alexMedia.sourceRevision)
        assertEquals(repository.snapshot.value.revision, alexMedia.mediaRevision)
    }

    @Test
    fun mergedCatalogReadsAreStableAndKeepExactProtocolModels() = runTest {
        val first = LocalSourceId("first-addon")
        val second = LocalSourceId("second-addon")
        val profile = HouseholdProfileId("alex")
        val repository = LocalFirstMediaRepository(InMemoryMediaStore())
        val firstPreview = MetaPreview(id = "same", type = "movie", name = "First")
        val secondPreview = MetaPreview(id = "same", type = "movie", name = "Second")
        repository.replaceSource(first, SourceMedia(stremioCatalogs = mapOf("popular" to listOf(firstPreview))))
        repository.replaceSource(second, SourceMedia(stremioCatalogs = mapOf("popular" to listOf(secondPreview))))
        val sourceState = LocalSourceState(profiles = listOf(source(second), source(first)))

        val view = repository.snapshot.value.visibleTo(profile, sourceState)

        assertEquals(listOf(secondPreview, firstPreview), view.mergedStremioCatalog("popular"))
        assertEquals(secondPreview, view.sources.first().media.stremioCatalogs.getValue("popular").single())
    }

    @Test
    fun equivalentUpdatesAndMissingRemovalDoNotChurnRevision() = runTest {
        val sourceId = LocalSourceId("playlist")
        val repository = LocalFirstMediaRepository(InMemoryMediaStore())
        val media = SourceMedia(
            stremioCatalogs = linkedMapOf(
                "z-last" to listOf(MetaPreview("z", "movie", "Z")),
                "a-first" to listOf(MetaPreview("a", "movie", "A")),
            ),
            liveChannels = listOf(channel("one")),
        )
        repository.replaceSource(sourceId, media)
        val revision = repository.snapshot.value.revision

        repository.replaceSource(
            sourceId,
            media.copy(stremioCatalogs = media.stremioCatalogs.entries.reversed().associate { it.toPair() }),
        )
        repository.removeSource(LocalSourceId("missing"))

        assertEquals(revision, repository.snapshot.value.revision)
        assertEquals(
            listOf("a-first", "z-last"),
            repository.snapshot.value.source(sourceId)?.stremioCatalogs?.keys?.toList(),
        )
    }

    @Test
    fun futureSyncIsLocalFirstAndIdempotent() = runTest {
        val id = LocalSourceId("sync")
        val store = InMemoryMediaStore(MediaSnapshot(revision = 4))
        val local = LocalFirstMediaRepository(store)
        assertEquals(MediaRefreshResult.LocalOnly, local.refresh())
        assertEquals(4, local.snapshot.value.revision)

        val synced = LocalFirstMediaRepository(store) { previous ->
            previous.copy(sources = mapOf(id to SourceMedia(liveChannels = listOf(channel("synced")))))
        }
        assertEquals(MediaRefreshResult.Updated(5), synced.refresh())
        assertIs<MediaRefreshResult.Unchanged>(synced.refresh())
        assertEquals(5, synced.snapshot.value.revision)
    }

    private fun source(
        id: LocalSourceId,
        scope: LocalSourceScope = LocalSourceScope.Global,
        enabled: Boolean = true,
    ) = LocalSourceProfile(id, id.value, LocalSourceKind.M3u, enabled, scope)

    private fun channel(name: String) = IptvChannel(
        id = ChannelId(name),
        name = name,
        streamUrl = "https://provider.invalid/$name.m3u8",
        source = IptvSourceKind.M3u,
        kind = ChannelKind.Live,
    )
}
