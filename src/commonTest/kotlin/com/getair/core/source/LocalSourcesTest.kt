package com.getair.core.source

import com.getair.core.household.HouseholdProfileId
import com.getair.iptv.XtreamCredentials
import com.getair.iptv.model.StreamFormat
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LocalSourcesTest {
    @Test
    fun filtersAllSourceKindsThroughOneProfileScope() = runTest {
        val alex = HouseholdProfileId("alex")
        val kids = HouseholdProfileId("kids")
        val state = LocalSourceState(
            profiles = listOf(
                LocalSourceProfile(LocalSourceId("global-addon"), "Global addon", LocalSourceKind.StremioAddon),
                LocalSourceProfile(
                    LocalSourceId("alex-xtream"),
                    "Alex TV",
                    LocalSourceKind.Xtream,
                    scope = LocalSourceScope.selectedProfiles(listOf(alex)),
                ),
                LocalSourceProfile(
                    LocalSourceId("kids-list"),
                    "Kids playlist",
                    LocalSourceKind.M3u,
                    enabled = false,
                    scope = LocalSourceScope.selectedProfiles(listOf(kids)),
                ),
            ),
        )

        assertEquals(listOf("global-addon", "alex-xtream"), state.sourcesFor(alex).map { it.id.value })
        assertEquals(listOf("global-addon"), state.sourcesFor(kids).map { it.id.value })
        assertEquals(
            listOf("global-addon", "kids-list"),
            state.sourcesFor(kids, includeDisabled = true).map { it.id.value },
        )
    }

    @Test
    fun scopeChangesAndProfileRemovalAreIdempotentWithoutWideningAccess() = runTest {
        val alex = HouseholdProfileId("alex")
        val kids = HouseholdProfileId("kids")
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), InMemoryLocalSourceSecretStore())
        val source = LocalSourceProfile(LocalSourceId("family-tv"), "Family TV", LocalSourceKind.M3u)
        registry.upsert(source, M3uSourceSecret("https://provider.invalid/list.m3u"))
        val selected = LocalSourceScope.selectedProfiles(listOf(alex, kids))

        registry.setScope(source.id, selected)
        val scopedRevision = registry.state.value.revision
        registry.setScope(source.id, selected)
        assertEquals(scopedRevision, registry.state.value.revision)

        registry.removeProfileAccess(alex)
        val afterRemoval = registry.state.value
        assertTrue(afterRemoval.sourcesFor(alex).isEmpty())
        assertEquals(listOf(source.id), afterRemoval.sourcesFor(kids).map(LocalSourceProfile::id))

        registry.removeProfileAccess(kids)
        val emptyScope = registry.state.value.profiles.single().scope
        assertEquals(LocalSourceScope.selectedProfiles(emptyList()), emptyScope)
        assertTrue(registry.state.value.sourcesFor(alex).isEmpty())
        assertTrue(registry.state.value.sourcesFor(kids).isEmpty())
        val emptyRevision = registry.state.value.revision
        registry.removeProfileAccess(kids)
        assertEquals(emptyRevision, registry.state.value.revision)
    }

    @Test
    fun keepsMultiplePlaylistsIndependent() = runTest {
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), InMemoryLocalSourceSecretStore())
        val news = LocalSourceProfile(LocalSourceId("news"), "News", LocalSourceKind.M3u)
        val sports = LocalSourceProfile(LocalSourceId("sports"), "Sports", LocalSourceKind.M3u)

        registry.upsert(news, M3uSourceSecret("https://news.invalid/list.m3u"))
        registry.upsert(sports, M3uSourceSecret("https://sports.invalid/list.m3u"))
        registry.setEnabled(news.id, false)

        assertEquals(listOf(news.id, sports.id), registry.state.value.profiles.map(LocalSourceProfile::id))
        assertFalse(registry.state.value.profiles.first { it.id == news.id }.enabled)
        assertIs<M3uSourceSecret>(registry.secret(news.id))
        assertIs<M3uSourceSecret>(registry.secret(sports.id))
    }

    @Test
    fun keepsExactCredentialsInARequiredLocalVault() = runTest {
        val vault = InMemoryLocalSourceSecretStore()
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), vault)
        val profile = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.Xtream)
        val credentials = XtreamCredentials(
            baseUrl = "https://provider.invalid:8080",
            username = "private-user",
            password = "private-password",
            preferredFormat = StreamFormat.M3u8,
        )
        val secret = XtreamSourceSecret(credentials)

        registry.upsert(profile, secret)

        val stored = assertIs<XtreamSourceSecret>(registry.secret(profile.id))
        assertEquals(credentials, stored.credentials)
        assertFalse("private-user" in registry.toString())
        assertFalse("private-password" in vault.toString())
        assertFalse("private-user" in secret.toString())
    }

    @Test
    fun metadataChangesNeverLeaveOldCredentialsBehind() = runTest {
        val vault = InMemoryLocalSourceSecretStore()
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), vault)
        val first = LocalSourceProfile(LocalSourceId("one"), "Living TV", LocalSourceKind.M3u)
        val replacement = LocalSourceProfile(LocalSourceId("two"), "Living TV", LocalSourceKind.M3u)
        registry.upsert(first, M3uSourceSecret("https://first.invalid/list.m3u"))

        registry.upsert(replacement, M3uSourceSecret("https://second.invalid/list.m3u"))

        assertNull(vault.read(first.id))
        assertIs<M3uSourceSecret>(vault.read(replacement.id))
        registry.remove(replacement.id)
        assertNull(vault.read(replacement.id))
    }

    @Test
    fun sameEnabledStateAndMissingRemovalDoNotChurnRevision() = runTest {
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), InMemoryLocalSourceSecretStore())
        val profile = LocalSourceProfile(LocalSourceId("addon"), "Metadata", LocalSourceKind.StremioAddon)
        registry.upsert(profile, StremioAddonSourceSecret("https://metadata.invalid/manifest.json"))
        val revision = registry.state.value.revision

        registry.setEnabled(profile.id, true)
        registry.remove(LocalSourceId("missing"))

        assertEquals(revision, registry.state.value.revision)
        assertIs<SourceRefreshResult.LocalOnly>(registry.refreshMetadata())
    }

    @Test
    fun reconstructedEquivalentCredentialDoesNotChurnRevision() = runTest {
        val registry = LocalSourceRegistry(InMemoryLocalSourceStore(), InMemoryLocalSourceSecretStore())
        val profile = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.Xtream)
        fun secret() = XtreamSourceSecret(
            XtreamCredentials("https://provider.invalid", "user", "password", StreamFormat.M3u8),
        )

        registry.upsert(profile, secret())
        val revision = registry.state.value.revision
        registry.upsert(profile, secret())

        assertEquals(revision, registry.state.value.revision)
    }
}
