package com.getair.core.source

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SourceRefreshCoordinatorTest {
    @Test
    fun boundsGlobalConcurrencyWithoutSerializingOtherSources() = runTest {
        var active = 0
        var peak = 0
        val gates = (1..4).associate { index -> source(index) to CompletableDeferred<Unit>() }
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            maxConcurrentRefreshes = 2,
            task = SourceRefreshTask { id ->
                active++
                peak = maxOf(peak, active)
                try {
                    gates.getValue(id).await()
                } finally {
                    active--
                }
            },
        )

        gates.keys.forEach { coordinator.request(it) }
        runCurrent()

        assertEquals(2, active)
        assertEquals(2, peak)
        assertEquals(2, coordinator.states.value.values.count { it.phase == SourceRefreshPhase.Queued })

        gates.getValue(source(1)).complete(Unit)
        runCurrent()
        assertEquals(2, active)
        assertEquals(2, peak)

        gates.values.forEach { it.complete(Unit) }
        runCurrent()
        assertEquals(0, active)
        assertEquals(2, peak)
        assertTrue(coordinator.states.value.values.all { it.phase == SourceRefreshPhase.Succeeded })
    }

    @Test
    fun aSlowSourceDoesNotBlockCachedReadsOrAnotherCompletion() = runTest {
        val slow = LocalSourceId("slow")
        val fast = LocalSourceId("fast")
        val slowGate = CompletableDeferred<Unit>()
        val cachedChannels = mapOf(slow to listOf("cached-news"), fast to listOf("cached-sports"))
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            maxConcurrentRefreshes = 2,
            task = SourceRefreshTask { id -> if (id == slow) slowGate.await() },
        )

        coordinator.request(slow)
        coordinator.request(fast)
        runCurrent()

        assertEquals(listOf("cached-news"), cachedChannels[slow])
        assertEquals(SourceRefreshPhase.Running, coordinator.stateOf(slow).phase)
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(fast).phase)

        slowGate.complete(Unit)
        runCurrent()
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(slow).phase)
    }

    @Test
    fun sourceCancellationAndRemovalAreIsolated() = runTest {
        val removed = LocalSourceId("removed")
        val retained = LocalSourceId("retained")
        val removedStarted = CompletableDeferred<Unit>()
        val removedCancelled = CompletableDeferred<Unit>()
        val retainedGate = CompletableDeferred<Unit>()
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            maxConcurrentRefreshes = 2,
            task = SourceRefreshTask { id ->
                if (id == removed) {
                    removedStarted.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        removedCancelled.complete(Unit)
                    }
                } else {
                    retainedGate.await()
                }
            },
        )

        coordinator.request(removed)
        coordinator.request(retained)
        runCurrent()
        removedStarted.await()

        coordinator.cancel(removed)
        runCurrent()
        assertTrue(removedCancelled.isCompleted)
        assertEquals(SourceRefreshPhase.Cancelled, coordinator.stateOf(removed).phase)
        assertEquals(SourceRefreshPhase.Running, coordinator.stateOf(retained).phase)

        coordinator.remove(removed)
        assertNull(coordinator.states.value[removed])

        retainedGate.complete(Unit)
        runCurrent()
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(retained).phase)
    }

    @Test
    fun removalWaitsForCancellationInsensitiveNativeCompletionBeforeCacheDeletion() = runTest {
        val source = LocalSourceId("native-refresh")
        val started = CompletableDeferred<Unit>()
        val nativeCompletion = CompletableDeferred<Unit>()
        val cache = mutableMapOf<LocalSourceId, String>()
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            task = SourceRefreshTask { id ->
                started.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    withContext(NonCancellable) {
                        nativeCompletion.await()
                        cache[id] = "late-native-publish"
                    }
                }
            },
        )
        coordinator.request(source)
        runCurrent()
        started.await()

        val removal = launch { coordinator.remove(source) }
        runCurrent()

        assertTrue(removal.isActive)
        assertNull(coordinator.states.value[source])
        assertNull(cache[source])

        nativeCompletion.complete(Unit)
        removal.join()
        cache.remove(source)
        runCurrent()

        assertFalse(removal.isActive)
        assertNull(coordinator.states.value[source])
        assertNull(cache[source])
    }

    @Test
    fun queuedDuplicateRequestsCoalesceIntoOneDeterministicRun() = runTest {
        val blocker = LocalSourceId("blocker")
        val duplicate = LocalSourceId("duplicate")
        val blockerGate = CompletableDeferred<Unit>()
        var duplicateRuns = 0
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            maxConcurrentRefreshes = 1,
            task = SourceRefreshTask { id ->
                if (id == blocker) blockerGate.await() else duplicateRuns++
            },
        )

        assertEquals(SourceRefreshRequest.Scheduled, coordinator.request(blocker))
        assertEquals(SourceRefreshRequest.Scheduled, coordinator.request(duplicate))
        assertEquals(SourceRefreshRequest.Coalesced, coordinator.request(duplicate))
        assertEquals(SourceRefreshRequest.Coalesced, coordinator.request(duplicate))
        assertEquals(2, coordinator.stateOf(duplicate).coalescedRequests)

        runCurrent()
        assertEquals(0, duplicateRuns)
        blockerGate.complete(Unit)
        runCurrent()

        assertEquals(1, duplicateRuns)
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(duplicate).phase)
        assertEquals(0, coordinator.stateOf(duplicate).coalescedRequests)
    }

    @Test
    fun activeDuplicatesScheduleAtMostOneFollowUp() = runTest {
        val source = LocalSourceId("updated-source")
        val firstGate = CompletableDeferred<Unit>()
        var runs = 0
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            maxConcurrentRefreshes = 1,
            task = SourceRefreshTask {
                runs++
                if (runs == 1) firstGate.await()
            },
        )

        coordinator.request(source)
        runCurrent()
        assertEquals(SourceRefreshRequest.FollowUpScheduled, coordinator.request(source))
        assertEquals(SourceRefreshRequest.Coalesced, coordinator.request(source))
        assertTrue(coordinator.stateOf(source).followUpPending)

        firstGate.complete(Unit)
        runCurrent()
        assertEquals(2, runs)
        assertEquals(2, coordinator.stateOf(source).completedRuns)
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(source).phase)
    }

    @Test
    fun callerCancellationDoesNotCancelAcceptedRefreshWork() = runTest {
        val source = LocalSourceId("caller-independent")
        val started = CompletableDeferred<Unit>()
        val finish = CompletableDeferred<Unit>()
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            task = SourceRefreshTask {
                started.complete(Unit)
                finish.await()
            },
        )
        val caller = launch {
            coordinator.request(source)
            awaitCancellation()
        }

        runCurrent()
        started.await()
        caller.cancel()
        runCurrent()
        assertFalse(caller.isActive)
        assertEquals(SourceRefreshPhase.Running, coordinator.stateOf(source).phase)

        finish.complete(Unit)
        runCurrent()
        assertEquals(SourceRefreshPhase.Succeeded, coordinator.stateOf(source).phase)
    }

    @Test
    fun failureKeepsCacheAndExposesOnlyARedactedFailureKind() = runTest {
        val source = LocalSourceId("https://user:password@provider.example/playlist.m3u")
        val cache = mutableMapOf(source to listOf("existing-channel"))
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            task = SourceRefreshTask {
                throw IllegalStateException("https://user:password@provider.example/private-token")
            },
        )

        coordinator.request(source)
        runCurrent()

        assertEquals(listOf("existing-channel"), cache[source])
        val state = coordinator.stateOf(source)
        assertEquals(SourceRefreshPhase.Failed, state.phase)
        assertEquals(SourceRefreshFailure.TaskFailed, state.failure)
        assertFalse(state.toString().contains("provider.example"))
        assertFalse(coordinator.toString().contains("provider.example"))
        assertIs<SourceRefreshFailure>(state.failure)
    }

    @Test
    fun closeCancelsOwnedWorkButNotTheParentScope() = runTest {
        val source = LocalSourceId("closing")
        val coordinator = SourceRefreshCoordinator(
            scope = backgroundScope,
            task = SourceRefreshTask { awaitCancellation() },
        )
        coordinator.request(source)
        runCurrent()

        coordinator.close()

        assertEquals(SourceRefreshPhase.Cancelled, coordinator.stateOf(source).phase)
        assertTrue(backgroundScope.coroutineContext[kotlinx.coroutines.Job]!!.isActive)
    }

    private fun source(index: Int) = LocalSourceId("source-$index")
}
