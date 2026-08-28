package com.getair.core.persistence

import com.getair.core.household.HouseholdProfile
import com.getair.core.household.HouseholdProfileId
import com.getair.core.source.InMemoryLocalSourceSecretStore
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
}
