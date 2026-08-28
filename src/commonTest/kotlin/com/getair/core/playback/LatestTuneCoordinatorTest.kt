package com.getair.core.playback

import com.getair.core.source.LocalSourceId
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class LatestTuneCoordinatorTest {
    private val source = LocalSourceId("provider-one")

    @Test
    fun hundredRequestBurstPerformsOnePlayerOpen() = runTest {
        val opened = mutableListOf<String>()
        val connections = BoundedSourceConnectionLeaser()
        val coordinator = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> { it.targetId.value },
            opener = PlaybackSessionOpener { target ->
                opened += target
                PlaybackSession { }
            },
        )

        val outcomes = List(100) { index ->
            async(start = CoroutineStart.UNDISPATCHED) {
                coordinator.tune(request(index.toString()))
            }
        }.map { it.await() }

        assertEquals(99, outcomes.count { it is TuneOutcome.Superseded })
        assertIs<TuneOutcome.Playing>(outcomes.last())
        assertEquals(listOf("99"), opened)
        assertEquals(1, connections.usage(source).activeConnections)
        assertIs<StopOutcome.Stopped>(coordinator.stop())
    }

    @Test
    fun rapidTuningOpensOnlyTheLatestViableChannel() = runTest {
        val aStarted = CompletableDeferred<Unit>()
        val bStarted = CompletableDeferred<Unit>()
        val opened = mutableListOf<String>()
        val connections = BoundedSourceConnectionLeaser()
        val coordinator = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> { request ->
                when (request.targetId.value) {
                    "a" -> {
                        aStarted.complete(Unit)
                        awaitCancellation()
                    }
                    "b" -> {
                        bStarted.complete(Unit)
                        awaitCancellation()
                    }
                    else -> request.targetId.value
                }
            },
            opener = PlaybackSessionOpener { target ->
                opened += target
                PlaybackSession { }
            },
        )

        val a = async { coordinator.tune(request("a")) }
        aStarted.await()
        val b = async { coordinator.tune(request("b")) }
        bStarted.await()
        val c = async { coordinator.tune(request("c")) }

        assertIs<TuneOutcome.Superseded>(a.await())
        assertIs<TuneOutcome.Superseded>(b.await())
        assertIs<TuneOutcome.Playing>(c.await())
        assertEquals(listOf("c"), opened)
        assertEquals(1, connections.usage(source).activeConnections)
        assertIs<StopOutcome.Stopped>(coordinator.stop())
    }

    @Test
    fun nativeCompletionAfterCancellationIsClosedBeforeLatestOpen() = runTest {
        val aOpening = CompletableDeferred<Unit>()
        var resumeA: Continuation<Unit>? = null
        val events = mutableListOf<String>()
        val connections = BoundedSourceConnectionLeaser()
        val coordinator = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> { it.targetId.value },
            opener = PlaybackSessionOpener { target ->
                if (target == "a") {
                    aOpening.complete(Unit)
                    suspendCoroutine { resumeA = it }
                }
                events += "open-$target"
                PlaybackSession { events += "close-$target" }
            },
        )

        val a = async { coordinator.tune(request("a")) }
        aOpening.await()
        val c = async(start = CoroutineStart.UNDISPATCHED) { coordinator.tune(request("c")) }
        resumeA?.resume(Unit)

        assertIs<TuneOutcome.Superseded>(a.await())
        assertIs<TuneOutcome.Playing>(c.await())
        assertEquals(listOf("open-a", "close-a", "open-c"), events)
        assertEquals(1, connections.usage(source).activeConnections)

        assertIs<StopOutcome.Stopped>(coordinator.stop())
        assertEquals(listOf("open-a", "close-a", "open-c", "close-c"), events)
        assertEquals(0, connections.usage(source).activeConnections)
    }

    @Test
    fun failedCloseRetainsTheLeaseAndBlocksReplacementUntilCleanupSucceeds() = runTest {
        var failFirstClose = true
        val opened = mutableListOf<String>()
        val connections = BoundedSourceConnectionLeaser()
        val coordinator = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> { it.targetId.value },
            opener = PlaybackSessionOpener { target ->
                opened += target
                PlaybackSession {
                    if (failFirstClose) {
                        failFirstClose = false
                        error("native close failed with https://user:password@example.invalid")
                    }
                }
            },
        )

        assertIs<TuneOutcome.Playing>(coordinator.tune(request("a")))
        val failed = assertIs<TuneOutcome.Failed>(coordinator.tune(request("b")))
        assertEquals(TuneFailureStage.CLOSE_PREVIOUS, failed.stage)
        assertEquals(listOf("a"), opened)
        assertEquals(1, connections.usage(source).activeConnections)

        assertIs<TuneOutcome.Playing>(coordinator.tune(request("b")))
        assertEquals(listOf("a", "b"), opened)
        assertEquals(1, connections.usage(source).activeConnections)
        assertIs<StopOutcome.Stopped>(coordinator.stop())
    }

    @Test
    fun providerLimitRejectionAndFailuresNeverExposeTargetsOrExceptions() = runTest {
        val connections = BoundedSourceConnectionLeaser()
        val recording = requireNotNull(
            connections.tryAcquire(source, SourceConnectionRole.RECORDING),
        )
        val blocked = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> { "https://user:password@example.invalid/live" },
            opener = PlaybackSessionOpener<String> { PlaybackSession { } },
        )

        val rejected = assertIs<TuneOutcome.Rejected>(blocked.tune(request("channel")))
        assertEquals(TuneRejectionReason.SOURCE_CONNECTION_LIMIT, rejected.reason)
        assertFalse(rejected.toString().contains("password"))
        recording.release()

        val failing = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> {
                error("https://user:password@example.invalid/live")
            },
            opener = PlaybackSessionOpener<String> { PlaybackSession { } },
        )
        val failed = assertIs<TuneOutcome.Failed>(failing.tune(request("channel")))
        assertEquals(TuneFailureStage.RESOLVE_TARGET, failed.stage)
        assertFalse(failed.toString().contains("password"))
        assertFalse(TuneTargetId("https://user:password@example.invalid").toString().contains("password"))
        assertFalse(source.toString().contains("provider-one"))
    }

    @Test
    fun callerCancellationIsPreservedAndDoesNotConsumeAConnection() = runTest {
        val resolving = CompletableDeferred<Unit>()
        var opened = false
        val connections = BoundedSourceConnectionLeaser()
        val coordinator = LatestTuneCoordinator(
            connections = connections,
            resolver = TuneTargetResolver<String> {
                resolving.complete(Unit)
                awaitCancellation()
            },
            opener = PlaybackSessionOpener<String> {
                opened = true
                PlaybackSession { }
            },
        )

        val caller = launch { coordinator.tune(request("a")) }
        resolving.await()
        caller.cancelAndJoin()

        assertTrue(caller.isCancelled)
        assertFalse(opened)
        assertEquals(0, connections.usage(source).activeConnections)
    }

    private fun request(target: String) = TuneRequest(source, TuneTargetId(target))
}
