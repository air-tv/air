package com.getair.core.persistence

import com.getair.core.household.HouseholdProfile
import com.getair.core.household.HouseholdProfileId
import com.getair.core.source.InMemoryLocalSourceSecretStore
import com.getair.core.source.LocalSourceId
import com.getair.core.source.LocalSourceKind
import com.getair.core.source.LocalSourceProfile
import com.getair.core.source.LocalSourceScope
import com.getair.core.source.M3uSourceSecret
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class LocalApplicationStateTest {
    @Test
    fun oneFactoryOpensAllLocalRepositoriesWithoutAServer() = runTest {
        val documents = InMemoryDocumentStore()
        val profile = HouseholdProfile(HouseholdProfileId("alex"), "Alex", "AX")

        val first = openLocalApplicationState(documents, InMemoryLocalSourceSecretStore())
        first.household.upsertProfile(profile)

        val reopened = openLocalApplicationState(documents, InMemoryLocalSourceSecretStore())
        assertEquals(profile, reopened.household.state.value.selectedProfile)
        assertIs<com.getair.core.household.HouseholdRefreshResult.LocalOnly>(reopened.household.refresh())
        assertIs<com.getair.core.source.SourceRefreshResult.LocalOnly>(reopened.sources.refreshMetadata())
        assertIs<com.getair.core.history.ContinueWatchingRefreshResult.LocalOnly>(
            reopened.continueWatching.refresh(),
        )
    }

    @Test
    fun profileDeletionDurablyPrunesSourceAccess() = runTest {
        val documents = InMemoryDocumentStore()
        val secrets = InMemoryLocalSourceSecretStore()
        val alex = HouseholdProfile(HouseholdProfileId("alex"), "Alex", "AX")
        val kids = HouseholdProfile(HouseholdProfileId("kids"), "Kids", "K")
        val source = LocalSourceProfile(
            id = LocalSourceId("shared-tv"),
            name = "Shared TV",
            kind = LocalSourceKind.M3u,
            scope = LocalSourceScope.selectedProfiles(listOf(alex.id, kids.id)),
        )
        val first = openLocalApplicationState(documents, secrets)
        first.household.upsertProfile(alex)
        first.household.upsertProfile(kids)
        first.sources.upsert(source, M3uSourceSecret("https://provider.invalid/list.m3u"))

        first.removeProfile(alex.id)
        val reopened = openLocalApplicationState(documents, secrets)

        assertEquals(listOf(kids.id), reopened.household.state.value.profiles.map { it.id })
        assertEquals(emptyList(), reopened.sources.state.value.sourcesFor(alex.id))
        assertEquals(listOf(source.id), reopened.sources.state.value.sourcesFor(kids.id).map { it.id })
        val revision = reopened.sources.state.value.revision
        reopened.removeProfile(alex.id)
        assertEquals(revision, reopened.sources.state.value.revision)
    }
}
