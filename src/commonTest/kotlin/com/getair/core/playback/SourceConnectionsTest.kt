package com.getair.core.playback

import com.getair.core.source.LocalSourceId
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SourceConnectionsTest {
    private val sourceA = LocalSourceId("source-a")
    private val sourceB = LocalSourceId("source-b")

    @Test
    fun oneSlotIsSharedByPlaybackPreviewRecordingAndFallback() = runTest {
        val connections = BoundedSourceConnectionLeaser()
        val playback = assertNotNull(
            connections.tryAcquire(sourceA, SourceConnectionRole.FOREGROUND_PLAYBACK),
        )

        SourceConnectionRole.entries
            .filterNot { it == SourceConnectionRole.FOREGROUND_PLAYBACK }
            .forEach { role -> assertNull(connections.tryAcquire(sourceA, role)) }

        assertEquals(1, connections.usage(sourceA).activeConnections)
        playback.release()
        playback.release()
        assertEquals(0, connections.usage(sourceA).activeConnections)
    }

    @Test
    fun limitsAreSourceScopedAndCanBeLoweredWithoutKillingActiveStreams() = runTest {
        val connections = BoundedSourceConnectionLeaser()
        connections.setMaxConnections(sourceA, 2)

        val first = assertNotNull(connections.tryAcquire(sourceA, SourceConnectionRole.RECORDING))
        val second = assertNotNull(connections.tryAcquire(sourceA, SourceConnectionRole.PREVIEW))
        assertNull(connections.tryAcquire(sourceA, SourceConnectionRole.FOREGROUND_PLAYBACK))
        assertNotNull(connections.tryAcquire(sourceB, SourceConnectionRole.FOREGROUND_PLAYBACK)).release()

        connections.setMaxConnections(sourceA, 1)
        assertEquals(2, connections.usage(sourceA).activeConnections)
        assertNull(connections.tryAcquire(sourceA, SourceConnectionRole.FALLBACK_RETRY))

        first.release()
        assertNull(connections.tryAcquire(sourceA, SourceConnectionRole.FOREGROUND_PLAYBACK))
        second.release()
        assertNotNull(
            connections.tryAcquire(sourceA, SourceConnectionRole.FOREGROUND_PLAYBACK),
        ).release()
    }

    @Test
    fun concurrentAcquisitionNeverExceedsTheConfiguredBound() = runTest {
        val connections = BoundedSourceConnectionLeaser()
        connections.setMaxConnections(sourceA, 3)

        val attempts = List(100) {
            async { connections.tryAcquire(sourceA, SourceConnectionRole.RECORDING) }
        }.map { it.await() }

        assertEquals(3, attempts.count { it != null })
        assertEquals(3, connections.usage(sourceA).activeConnections)
        attempts.filterNotNull().forEach { it.release() }
    }

    @Test
    fun invalidLimitsFailBeforeMutatingState() = runTest {
        assertFailsWith<IllegalArgumentException> { BoundedSourceConnectionLeaser(0) }
        val connections = BoundedSourceConnectionLeaser()
        assertFailsWith<IllegalArgumentException> { connections.setMaxConnections(sourceA, 0) }
        assertEquals(1, connections.usage(sourceA).maxConnections)
    }
}
