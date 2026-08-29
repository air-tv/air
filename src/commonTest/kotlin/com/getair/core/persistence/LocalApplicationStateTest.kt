package com.getair.core.persistence

import com.getair.core.history.OnDemandContentRef
import com.getair.core.history.WatchProgress
import com.getair.core.household.HouseholdProfile
import com.getair.core.household.HouseholdProfileId
import com.getair.core.library.LibraryItemRef
import com.getair.core.source.InMemoryLocalSourceSecretStore
import com.getair.core.source.LocalSourceId
import com.getair.core.source.LocalSourceKind
import com.getair.core.source.LocalSourceProfile
import com.getair.core.source.LocalSourceScope
import com.getair.core.source.M3uSourceSecret
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

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

    @Test
    fun everyProfileCleanupFailureKeepsAHouseholdRetryHandleAcrossReopen() = runTest {
        removalDocuments.forEach { document ->
            val documents = FaultInjectingDocumentStore()
            val secrets = InMemoryLocalSourceSecretStore()
            val application = seedProfileRemovalState(documents, secrets)
            documents.failNextWrite(document)

            val failure = assertFailsWith<LocalProfileRemovalException> {
                application.removeProfile(alex.id)
            }

            assertRemovalFailureType(document, failure)
            assertFalse(PRIVATE_DIAGNOSTIC in failure.toString())
            assertFalse(alex.id.value in failure.toString())
            assertNull(failure.cause)
            assertTrue(application.household.state.value.profiles.any { it.id == alex.id })

            val reopened = openLocalApplicationState(documents, secrets)
            assertTrue(reopened.household.state.value.profiles.any { it.id == alex.id })
            reopened.removeProfile(alex.id)
            assertProfileRemovalComplete(reopened)
        }
    }

    @Test
    fun cancellationAtEveryProfileCleanupStepPropagatesAndRemainsRetryable() = runTest {
        removalDocuments.forEach { document ->
            val documents = FaultInjectingDocumentStore()
            val secrets = InMemoryLocalSourceSecretStore()
            val application = seedProfileRemovalState(documents, secrets)
            val cancellationStarted = documents.cancelNextWrite(document)

            val removal = async { application.removeProfile(alex.id) }
            cancellationStarted.await()
            removal.cancelAndJoin()

            assertTrue(removal.isCancelled)
            assertTrue(application.household.state.value.profiles.any { it.id == alex.id })

            val reopened = openLocalApplicationState(documents, secrets)
            reopened.removeProfile(alex.id)
            assertProfileRemovalComplete(reopened)
        }
    }

    @Test
    fun concurrentProfileRemovalSerializesEachDurableCleanupOnce() = runTest {
        val documents = FaultInjectingDocumentStore()
        val secrets = InMemoryLocalSourceSecretStore()
        val application = seedProfileRemovalState(documents, secrets)
        val baselineWrites = documents.writeCounts.toMap()
        val block = documents.blockNextWrite(SOURCES_DOCUMENT)

        val first = async { application.removeProfile(alex.id) }
        block.started.await()
        val concurrent = async { application.removeProfile(alex.id) }
        block.allow.complete(Unit)
        first.await()
        concurrent.await()

        assertProfileRemovalComplete(application)
        removalDocuments.forEach { document ->
            assertEquals(baselineWrites[document].orZero() + 1, documents.writeCounts[document].orZero())
        }
    }

    @Test
    fun missingProfileRemovalIsADurableNoOpIncludingLegacyPartialState() = runTest {
        val documents = FaultInjectingDocumentStore()
        val secrets = InMemoryLocalSourceSecretStore()
        val application = seedProfileRemovalState(documents, secrets)
        val baselineWrites = documents.writeCounts.toMap()

        application.removeProfile(HouseholdProfileId("missing"))

        assertEquals(baselineWrites, documents.writeCounts)

        // Repair the old household-first failure shape: profile metadata is
        // absent while its scoped content still exists in the other stores.
        application.household.removeProfile(alex.id)
        val reopened = openLocalApplicationState(documents, secrets)
        assertTrue(reopened.continueWatching.entries(alex.id).isNotEmpty())
        assertFalse(reopened.profileLibrary.profile(alex.id).isEmpty)
        reopened.removeProfile(alex.id)
        assertProfileRemovalComplete(reopened)
    }

    private suspend fun seedProfileRemovalState(
        documents: FaultInjectingDocumentStore,
        secrets: InMemoryLocalSourceSecretStore,
    ): LocalApplicationState {
        val application = openLocalApplicationState(documents, secrets)
        application.household.upsertProfile(alex)
        application.household.upsertProfile(kids)
        application.sources.upsert(
            source,
            M3uSourceSecret("https://provider.invalid/list.m3u"),
        )
        val content = OnDemandContentRef.Stremio("movie", "tt-profile-delete")
        application.continueWatching.record(
            alex.id,
            WatchProgress(content, positionMillis = 1_000, durationMillis = 10_000, updatedAtEpochMillis = 1),
        )
        application.profileLibrary.addFavorite(alex.id, LibraryItemRef.OnDemand(content), 1)
        return application
    }

    private fun assertProfileRemovalComplete(application: LocalApplicationState) {
        assertFalse(application.household.state.value.profiles.any { it.id == alex.id })
        assertTrue(application.continueWatching.entries(alex.id).isEmpty())
        assertTrue(application.profileLibrary.profile(alex.id).isEmpty)
        assertTrue(application.sources.state.value.sourcesFor(alex.id).isEmpty())
        assertEquals(listOf(source.id), application.sources.state.value.sourcesFor(kids.id).map { it.id })
        assertEquals(
            LocalSourceScope.selectedProfiles(listOf(kids.id)),
            application.sources.state.value.profiles.single().scope,
        )
    }

    private fun assertRemovalFailureType(document: String, failure: LocalProfileRemovalException) {
        when (document) {
            SOURCES_DOCUMENT -> assertIs<LocalProfileRemovalException.SourceAccessRemoval>(failure)
            CONTINUE_WATCHING_DOCUMENT -> assertIs<LocalProfileRemovalException.ContinueWatchingRemoval>(failure)
            PROFILE_LIBRARY_DOCUMENT -> assertIs<LocalProfileRemovalException.ProfileLibraryRemoval>(failure)
            HOUSEHOLD_DOCUMENT -> assertIs<LocalProfileRemovalException.HouseholdRecordRemoval>(failure)
        }
    }

    private companion object {
        val alex = HouseholdProfile(HouseholdProfileId("alex"), "Alex", "AX")
        val kids = HouseholdProfile(HouseholdProfileId("kids"), "Kids", "K")
        val source = LocalSourceProfile(
            id = LocalSourceId("shared-tv"),
            name = "Shared TV",
            kind = LocalSourceKind.M3u,
            scope = LocalSourceScope.selectedProfiles(listOf(alex.id, kids.id)),
        )
        const val SOURCES_DOCUMENT = "sources.v1"
        const val CONTINUE_WATCHING_DOCUMENT = "continue-watching.v1"
        const val PROFILE_LIBRARY_DOCUMENT = "profile-library.v1"
        const val HOUSEHOLD_DOCUMENT = "household.v1"
        val removalDocuments = listOf(
            SOURCES_DOCUMENT,
            CONTINUE_WATCHING_DOCUMENT,
            PROFILE_LIBRARY_DOCUMENT,
            HOUSEHOLD_DOCUMENT,
        )
    }
}

private class FaultInjectingDocumentStore : LocalDocumentStore {
    private val delegate = InMemoryDocumentStore()
    private var failedDocument: String? = null
    private var cancelledDocument: String? = null
    private var cancellationStarted: CompletableDeferred<Unit>? = null
    private var blockedDocument: String? = null
    private var block: WriteBlock? = null
    val writeCounts = mutableMapOf<String, Int>()

    override suspend fun read(document: String): String? = delegate.read(document)

    override suspend fun write(document: String, value: String) {
        writeCounts[document] = writeCounts[document].orZero() + 1
        if (failedDocument == document) {
            failedDocument = null
            throw IllegalStateException("write failed: $PRIVATE_DIAGNOSTIC")
        }
        if (cancelledDocument == document) {
            cancelledDocument = null
            cancellationStarted?.complete(Unit)
            awaitCancellation()
        }
        if (blockedDocument == document) {
            blockedDocument = null
            block?.started?.complete(Unit)
            block?.allow?.await()
        }
        delegate.write(document, value)
    }

    override suspend fun remove(document: String) = delegate.remove(document)

    fun failNextWrite(document: String) {
        failedDocument = document
    }

    fun cancelNextWrite(document: String): CompletableDeferred<Unit> {
        cancelledDocument = document
        return CompletableDeferred<Unit>().also { cancellationStarted = it }
    }

    fun blockNextWrite(document: String): WriteBlock {
        blockedDocument = document
        return WriteBlock().also { block = it }
    }
}

private class WriteBlock(
    val started: CompletableDeferred<Unit> = CompletableDeferred(),
    val allow: CompletableDeferred<Unit> = CompletableDeferred(),
)

private fun Int?.orZero(): Int = this ?: 0

private const val PRIVATE_DIAGNOSTIC = "https://user:private-password@provider.invalid/list.m3u"
