package com.getair.core.source

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Performs one source-scoped refresh using the latest source configuration. */
fun interface SourceRefreshTask {
    suspend fun refresh(sourceId: LocalSourceId)
}

enum class SourceRefreshPhase {
    Idle,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/** Deliberately contains no provider exception, URL, credential, or response body. */
enum class SourceRefreshFailure {
    TaskFailed,
}

data class SourceRefreshState(
    val phase: SourceRefreshPhase,
    val completedRuns: Long = 0,
    val coalescedRequests: Int = 0,
    val followUpPending: Boolean = false,
    val failure: SourceRefreshFailure? = null,
) {
    companion object {
        val Idle = SourceRefreshState(SourceRefreshPhase.Idle)
    }
}

enum class SourceRefreshRequest {
    Scheduled,
    FollowUpScheduled,
    Coalesced,
}

/**
 * Coordinates refresh work without knowing anything about Xtream, M3U,
 * Stalker, or Stremio transports.
 *
 * The injected [task] is expected to build new source data separately and
 * publish it atomically only after a successful refresh. This coordinator
 * never clears media, guide, search, or artwork caches when a task fails.
 *
 * Requests queued for the same source share one run. A request received while
 * that source is running schedules exactly one follow-up run, so a source
 * configuration change cannot be lost without allowing an unbounded backlog.
 */
class SourceRefreshCoordinator(
    scope: CoroutineScope,
    private val task: SourceRefreshTask,
    val maxConcurrentRefreshes: Int = 2,
) {
    init {
        require(maxConcurrentRefreshes > 0) { "Refresh concurrency must be positive" }
    }

    private val coordinatorJob = SupervisorJob(scope.coroutineContext[Job])
    private val coordinatorScope = CoroutineScope(
        scope.coroutineContext + coordinatorJob + CoroutineName("air-source-refresh"),
    )
    private val lock = Mutex()
    private val entries = mutableMapOf<LocalSourceId, Entry>()
    private val pending = ArrayDeque<Ticket>()
    private val _states = MutableStateFlow<Map<LocalSourceId, SourceRefreshState>>(emptyMap())
    private var activeCount = 0
    private var nextGeneration = 1L
    private var nextTaskSequence = 1L
    private var closed = false

    val states: StateFlow<Map<LocalSourceId, SourceRefreshState>> = _states.asStateFlow()

    fun stateOf(sourceId: LocalSourceId): SourceRefreshState =
        states.value[sourceId] ?: SourceRefreshState.Idle

    suspend fun request(sourceId: LocalSourceId): SourceRefreshRequest = lock.withLock {
        check(!closed && coordinatorJob.isActive) { "Source refresh coordinator is closed" }
        val current = entries[sourceId]
        val result = when (current?.phase) {
            SourceRefreshPhase.Queued -> {
                current.coalescedRequests++
                publishLocked(sourceId, current)
                SourceRefreshRequest.Coalesced
            }

            SourceRefreshPhase.Running -> {
                if (current.followUpPending) {
                    current.coalescedRequests++
                    publishLocked(sourceId, current)
                    SourceRefreshRequest.Coalesced
                } else {
                    current.followUpPending = true
                    publishLocked(sourceId, current)
                    SourceRefreshRequest.FollowUpScheduled
                }
            }

            else -> {
                val next = current ?: Entry()
                next.generation = nextGeneration++
                next.phase = SourceRefreshPhase.Queued
                next.coalescedRequests = 0
                next.followUpPending = false
                next.failure = null
                entries[sourceId] = next
                pending.addLast(Ticket(sourceId, next.generation))
                publishLocked(sourceId, next)
                SourceRefreshRequest.Scheduled
            }
        }
        launchAvailableLocked()
        result
    }

    /** Cancels only this source. Other active and queued sources are untouched. */
    suspend fun cancel(sourceId: LocalSourceId) {
        val job = lock.withLock {
            val entry = entries[sourceId] ?: return
            pending.removeAll { it.sourceId == sourceId && it.generation == entry.generation }
            val active = entry.activeJob
            entry.generation = nextGeneration++
            entry.phase = SourceRefreshPhase.Cancelled
            entry.activeJob = null
            entry.followUpPending = false
            entry.coalescedRequests = 0
            entry.failure = null
            publishLocked(sourceId, entry)
            launchAvailableLocked()
            active
        }
        job?.cancel(CancellationException("Source refresh cancelled"))
    }

    /** Cancels refresh work and removes all observable scheduler state for a deleted source. */
    suspend fun remove(sourceId: LocalSourceId) {
        val job = lock.withLock {
            val entry = entries.remove(sourceId) ?: return
            pending.removeAll { it.sourceId == sourceId && it.generation == entry.generation }
            entry.generation = nextGeneration++
            _states.value = _states.value - sourceId
            launchAvailableLocked()
            entry.activeJob
        }
        withContext(NonCancellable) {
            job?.cancelAndJoin()
        }
    }

    /** Stops coordinator-owned work without cancelling the caller's scope. */
    suspend fun close() {
        val shouldClose = lock.withLock {
            if (closed) return
            closed = true
            pending.clear()
            entries.forEach { (sourceId, entry) ->
                entry.generation = nextGeneration++
                if (entry.phase == SourceRefreshPhase.Queued || entry.phase == SourceRefreshPhase.Running) {
                    entry.phase = SourceRefreshPhase.Cancelled
                    entry.followUpPending = false
                    entry.failure = null
                    publishLocked(sourceId, entry)
                }
            }
            true
        }
        if (shouldClose) coordinatorJob.cancelAndJoin()
    }

    private fun launchAvailableLocked() {
        while (!closed && activeCount < maxConcurrentRefreshes) {
            val ticket = pending.removeFirstOrNull() ?: return
            val entry = entries[ticket.sourceId]
            if (entry == null || entry.generation != ticket.generation || entry.phase != SourceRefreshPhase.Queued) {
                continue
            }
            entry.phase = SourceRefreshPhase.Running
            entry.failure = null
            activeCount++
            val sequence = nextTaskSequence++
            val job = coordinatorScope.launch(CoroutineName("air-source-refresh-task-$sequence")) {
                execute(ticket)
            }
            entry.activeJob = job
            publishLocked(ticket.sourceId, entry)
        }
    }

    private suspend fun execute(ticket: Ticket) {
        val outcome = try {
            task.refresh(ticket.sourceId)
            Outcome.Succeeded
        } catch (_: CancellationException) {
            Outcome.Cancelled
        } catch (_: Throwable) {
            Outcome.Failed
        }

        withContext(NonCancellable) {
            lock.withLock {
                activeCount--
                val entry = entries[ticket.sourceId]
                if (entry != null && entry.generation == ticket.generation) {
                    entry.activeJob = null
                    entry.completedRuns++
                    if (entry.followUpPending && outcome != Outcome.Cancelled && !closed) {
                        entry.phase = SourceRefreshPhase.Queued
                        entry.followUpPending = false
                        pending.addLast(ticket)
                    } else {
                        entry.followUpPending = false
                        entry.coalescedRequests = 0
                        entry.phase = when (outcome) {
                            Outcome.Succeeded -> SourceRefreshPhase.Succeeded
                            Outcome.Failed -> SourceRefreshPhase.Failed
                            Outcome.Cancelled -> SourceRefreshPhase.Cancelled
                        }
                        entry.failure = if (outcome == Outcome.Failed) {
                            SourceRefreshFailure.TaskFailed
                        } else {
                            null
                        }
                    }
                    publishLocked(ticket.sourceId, entry)
                }
                launchAvailableLocked()
            }
        }
    }

    private fun publishLocked(sourceId: LocalSourceId, entry: Entry) {
        _states.value = _states.value + (sourceId to entry.snapshot())
    }

    override fun toString(): String =
        "SourceRefreshCoordinator(maxConcurrentRefreshes=$maxConcurrentRefreshes, task=<redacted>)"

    private data class Ticket(
        val sourceId: LocalSourceId,
        val generation: Long,
    )

    private class Entry {
        var generation: Long = 0
        var phase: SourceRefreshPhase = SourceRefreshPhase.Idle
        var completedRuns: Long = 0
        var coalescedRequests: Int = 0
        var followUpPending: Boolean = false
        var failure: SourceRefreshFailure? = null
        var activeJob: Job? = null

        fun snapshot() = SourceRefreshState(
            phase = phase,
            completedRuns = completedRuns,
            coalescedRequests = coalescedRequests,
            followUpPending = followUpPending,
            failure = failure,
        )
    }

    private enum class Outcome {
        Succeeded,
        Failed,
        Cancelled,
    }
}
