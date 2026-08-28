package com.getair.core.source

import com.getair.iptv.XtreamCredentials
import com.getair.iptv.model.StreamFormat
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull

class LocalSourcesTest {
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
}
