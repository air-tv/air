package com.getair.core.source

import com.getair.core.household.HouseholdProfileId
import com.getair.core.persistence.InMemoryDocumentStore
import com.getair.core.persistence.PersistentLocalSourceStore
import com.getair.iptv.XtreamCredentials
import com.getair.iptv.model.StreamFormat
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    fun vaultFailureKeepsDurableMetadataSoRemovalCanRetryAfterReopen() = runTest {
        val documents = InMemoryDocumentStore()
        val store = PersistentLocalSourceStore.open(documents)
        val vault = ControllableSecretStore()
        val source = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.M3u)
        val registry = LocalSourceRegistry(store, vault)
        registry.upsert(source, M3uSourceSecret("https://provider.invalid/private.m3u"))
        val revision = registry.state.value.revision
        vault.removeFailure = IllegalStateException("vault failed with private.m3u")

        assertIs<LocalSourceRemovalException.CredentialStore>(
            assertFailsWith<LocalSourceRemovalException> { registry.remove(source.id) },
        )

        assertEquals(listOf(source), registry.state.value.profiles)
        assertEquals(revision, registry.state.value.revision)
        assertIs<M3uSourceSecret>(vault.read(source.id))

        val reopenedStore = PersistentLocalSourceStore.open(documents)
        LocalSourceRegistry(reopenedStore, vault).remove(source.id)

        assertTrue(reopenedStore.state.value.profiles.isEmpty())
        assertEquals(revision + 1, reopenedStore.state.value.revision)
        assertNull(vault.read(source.id))
    }

    @Test
    fun metadataFailureLeavesRetryHandleAfterCredentialIsGone() = runTest {
        val store = ControllableSourceStore()
        val vault = ControllableSecretStore()
        val source = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.M3u)
        val registry = LocalSourceRegistry(store, vault)
        registry.upsert(source, M3uSourceSecret("https://provider.invalid/private.m3u"))
        val revision = registry.state.value.revision
        store.replaceFailure = IllegalStateException("disk failed with private.m3u")

        assertIs<LocalSourceRemovalException.MetadataStore>(
            assertFailsWith<LocalSourceRemovalException> { registry.remove(source.id) },
        )

        assertEquals(listOf(source), registry.state.value.profiles)
        assertEquals(revision, registry.state.value.revision)
        assertNull(vault.read(source.id))

        LocalSourceRegistry(store, vault).remove(source.id)

        assertTrue(store.state.value.profiles.isEmpty())
        assertEquals(revision + 1, store.state.value.revision)
    }

    @Test
    fun cancelledPartialVaultRemovalRemainsRetryable() = runTest {
        val store = ControllableSourceStore()
        val vault = ControllableSecretStore()
        val source = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.M3u)
        val registry = LocalSourceRegistry(store, vault)
        registry.upsert(source, M3uSourceSecret("https://provider.invalid/private.m3u"))
        val removedFromVault = CompletableDeferred<Unit>()
        vault.afterRemove = {
            removedFromVault.complete(Unit)
            awaitCancellation()
        }

        val removal = async { registry.remove(source.id) }
        removedFromVault.await()
        removal.cancelAndJoin()

        assertEquals(listOf(source), store.state.value.profiles)
        assertNull(vault.read(source.id))
        vault.afterRemove = null

        LocalSourceRegistry(store, vault).remove(source.id)

        assertTrue(store.state.value.profiles.isEmpty())
    }

    @Test
    fun concurrentAndMissingRemovalAreSerializedIdempotentNoOps() = runTest {
        val store = ControllableSourceStore()
        val vault = ControllableSecretStore()
        val source = LocalSourceProfile(LocalSourceId("living-tv"), "Living TV", LocalSourceKind.M3u)
        val registry = LocalSourceRegistry(store, vault)
        registry.upsert(source, M3uSourceSecret("https://provider.invalid/private.m3u"))
        val removalStarted = CompletableDeferred<Unit>()
        val allowRemoval = CompletableDeferred<Unit>()
        vault.afterRemove = {
            removalStarted.complete(Unit)
            allowRemoval.await()
        }

        val first = async { registry.remove(source.id) }
        removalStarted.await()
        val concurrent = async { registry.remove(source.id) }
        assertEquals(listOf(source), registry.state.value.profiles)
        allowRemoval.complete(Unit)
        first.await()
        concurrent.await()
        val revision = store.state.value.revision
        val replaceCalls = store.replaceCalls
        registry.remove(source.id)
        registry.remove(LocalSourceId("missing"))

        assertEquals(1, vault.removeCalls)
        assertEquals(replaceCalls, store.replaceCalls)
        assertEquals(revision, store.state.value.revision)
        assertTrue(store.state.value.profiles.isEmpty())
    }

    @Test
    fun removalFailuresDoNotRetainProviderDiagnostics() = runTest {
        val secretText = "https://user:private-password@provider.invalid/list.m3u"
        val source = LocalSourceProfile(LocalSourceId("private-source-id"), "Living TV", LocalSourceKind.M3u)

        suspend fun failureFrom(
            configure: (ControllableSourceStore, ControllableSecretStore) -> Unit,
        ): LocalSourceRemovalException {
            val store = ControllableSourceStore()
            val vault = ControllableSecretStore()
            val registry = LocalSourceRegistry(store, vault)
            registry.upsert(source, M3uSourceSecret(secretText))
            configure(store, vault)
            return assertFailsWith<LocalSourceRemovalException> { registry.remove(source.id) }
        }

        val credentialFailure = failureFrom { _, vault ->
            vault.removeFailure = IllegalStateException("credential failure: $secretText")
        }
        val metadataFailure = failureFrom { store, _ ->
            store.replaceFailure = IllegalStateException("metadata failure: $secretText")
        }

        listOf(credentialFailure, metadataFailure).forEach { failure ->
            assertFalse(secretText in failure.toString())
            assertFalse("private-password" in failure.toString())
            assertFalse("private-source-id" in failure.toString())
            assertNull(failure.cause)
        }
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

private class ControllableSourceStore(
    initial: LocalSourceState = LocalSourceState(),
) : LocalSourceStore {
    private val mutableState = MutableStateFlow(initial)
    override val state: StateFlow<LocalSourceState> = mutableState.asStateFlow()
    var replaceFailure: Exception? = null
    var replaceCalls: Int = 0
        private set

    override suspend fun replace(state: LocalSourceState) {
        replaceCalls++
        replaceFailure?.let { failure ->
            replaceFailure = null
            throw failure
        }
        mutableState.value = state
    }
}

private class ControllableSecretStore : LocalSourceSecretStore {
    private val values = mutableMapOf<LocalSourceId, LocalSourceSecret>()
    var removeFailure: Exception? = null
    var afterRemove: (suspend () -> Unit)? = null
    var removeCalls: Int = 0
        private set

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = values[id]

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        values[id] = secret
    }

    override suspend fun remove(id: LocalSourceId) {
        removeCalls++
        removeFailure?.let { failure ->
            removeFailure = null
            throw failure
        }
        values.remove(id)
        afterRemove?.invoke()
    }
}
