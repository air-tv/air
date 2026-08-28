package com.getair.core.playback

import com.getair.core.source.LocalSourceId
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** The purpose of a provider connection, used for accounting rather than priority. */
enum class SourceConnectionRole {
    FOREGROUND_PLAYBACK,
    PREVIEW,
    RECORDING,
    FALLBACK_RETRY,
}

/**
 * A single connection slot for [sourceId]. Implementations must make [release] idempotent.
 *
 * A lease is only accounting: the owner must close its stream before releasing the lease.
 */
interface SourceConnectionLease {
    val sourceId: LocalSourceId
    val role: SourceConnectionRole

    suspend fun release()
}

interface SourceConnectionLeaser {
    /** Returns null immediately when the configured per-source limit is already in use. */
    suspend fun tryAcquire(
        sourceId: LocalSourceId,
        role: SourceConnectionRole,
    ): SourceConnectionLease?
}

data class SourceConnectionUsage(
    val maxConnections: Int,
    val activeByRole: Map<SourceConnectionRole, Int>,
) {
    val activeConnections: Int = activeByRole.values.sum()
}

/**
 * An in-memory, source-scoped provider connection budget.
 *
 * Limits default to one. Raising a limit affects the next acquisition. Lowering a limit never
 * tears down a live stream; new acquisitions are rejected until usage falls below the new limit.
 * Sources never share capacity, and roles never create separate capacity pools.
 */
class BoundedSourceConnectionLeaser(
    private val defaultMaxConnections: Int = 1,
) : SourceConnectionLeaser {
    private val mutex = Mutex()
    private val sources = mutableMapOf<LocalSourceId, SourceState>()
    private var nextLeaseId = 0L

    init {
        require(defaultMaxConnections >= 1) { "defaultMaxConnections must be at least one" }
    }

    suspend fun setMaxConnections(sourceId: LocalSourceId, maxConnections: Int) {
        require(maxConnections >= 1) { "maxConnections must be at least one" }
        mutex.withLock {
            state(sourceId).maxConnections = maxConnections
        }
    }

    suspend fun usage(sourceId: LocalSourceId): SourceConnectionUsage = mutex.withLock {
        val state = sources[sourceId]
        SourceConnectionUsage(
            maxConnections = state?.maxConnections ?: defaultMaxConnections,
            activeByRole = SourceConnectionRole.entries.associateWith { role ->
                state?.leases?.values?.count { it == role } ?: 0
            }.filterValues { it > 0 },
        )
    }

    override suspend fun tryAcquire(
        sourceId: LocalSourceId,
        role: SourceConnectionRole,
    ): SourceConnectionLease? = mutex.withLock {
        val state = state(sourceId)
        if (state.leases.size >= state.maxConnections) return@withLock null

        val leaseId = ++nextLeaseId
        state.leases[leaseId] = role
        Lease(sourceId, role, leaseId)
    }

    private fun state(sourceId: LocalSourceId): SourceState =
        sources.getOrPut(sourceId) { SourceState(defaultMaxConnections) }

    private inner class Lease(
        override val sourceId: LocalSourceId,
        override val role: SourceConnectionRole,
        private val leaseId: Long,
    ) : SourceConnectionLease {
        override suspend fun release() {
            mutex.withLock {
                sources[sourceId]?.leases?.remove(leaseId)
            }
        }
    }

    private class SourceState(
        var maxConnections: Int,
        val leases: MutableMap<Long, SourceConnectionRole> = mutableMapOf(),
    )
}
