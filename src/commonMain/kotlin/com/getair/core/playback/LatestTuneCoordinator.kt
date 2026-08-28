package com.getair.core.playback

import com.getair.core.source.LocalSourceId
import kotlin.jvm.JvmInline
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

@JvmInline
value class TuneTargetId(val value: String) {
    init {
        require(value.isNotBlank() && value.length <= 256 && '\u0000' !in value)
    }

    override fun toString(): String = "TuneTargetId(<redacted>)"
}

data class TuneRequest(
    val sourceId: LocalSourceId,
    val targetId: TuneTargetId,
    val role: SourceConnectionRole = SourceConnectionRole.FOREGROUND_PLAYBACK,
)

/** Resolves short-lived provider playback data without exposing it in coordinator outcomes. */
fun interface TuneTargetResolver<T : Any> {
    suspend fun resolve(request: TuneRequest): T
}

/** A specifically owned player session. Closing it must not stop a newer, unrelated session. */
fun interface PlaybackSession {
    /** Must be idempotent and return only after the stream no longer consumes a provider slot. */
    suspend fun close()
}

fun interface PlaybackSessionOpener<T : Any> {
    /**
     * Opens one session. If this throws, no live session may remain. A successful result owns every
     * resource opened by the call, even when cancellation arrived while a native call completed.
     */
    suspend fun open(target: T): PlaybackSession
}

enum class TuneFailureStage {
    CLOSE_PREVIOUS,
    RESOLVE_TARGET,
    OPEN_PLAYBACK,
}

enum class TuneRejectionReason {
    SOURCE_CONNECTION_LIMIT,
}

sealed interface TuneOutcome {
    val generation: Long

    data class Playing(
        override val generation: Long,
        val sourceId: LocalSourceId,
        val role: SourceConnectionRole,
    ) : TuneOutcome

    data class Superseded(override val generation: Long) : TuneOutcome

    data class Rejected(
        override val generation: Long,
        val reason: TuneRejectionReason,
    ) : TuneOutcome

    data class Failed(
        override val generation: Long,
        val stage: TuneFailureStage,
    ) : TuneOutcome
}

sealed interface StopOutcome {
    val generation: Long

    data class Stopped(override val generation: Long) : StopOutcome
    data class AlreadyStopped(override val generation: Long) : StopOutcome
    data class Superseded(override val generation: Long) : StopOutcome
    data class Failed(
        override val generation: Long,
        val stage: TuneFailureStage = TuneFailureStage.CLOSE_PREVIOUS,
    ) : StopOutcome
}

/**
 * Serializes player replacement while allowing callers to issue tune requests concurrently.
 *
 * Every request cancels the older tune generation. Native work that returns after cancellation is
 * closed before the newest request can open, so a stale completion cannot become the current
 * session. Resolver and opener implementations must preserve coroutine cancellation; the explicit
 * session handle is the safety net for native calls whose completion cannot be interrupted.
 */
class LatestTuneCoordinator<T : Any>(
    private val connections: SourceConnectionLeaser,
    private val resolver: TuneTargetResolver<T>,
    private val opener: PlaybackSessionOpener<T>,
) {
    private val generationMutex = Mutex()
    private val replacementMutex = Mutex()
    private var latestGeneration = 0L
    private var activeOperation: Job? = null
    private var currentSession: CurrentSession? = null

    suspend fun tune(request: TuneRequest): TuneOutcome = latest(
        superseded = TuneOutcome::Superseded,
    ) { generation ->
        replacementMutex.withLock {
            ensureCurrent(generation)

            when (closeCurrent(generation)) {
                CloseResult.CLOSED, CloseResult.NOTHING_TO_CLOSE -> Unit
                CloseResult.FAILED -> return@withLock TuneOutcome.Failed(
                    generation,
                    TuneFailureStage.CLOSE_PREVIOUS,
                )
            }
            ensureCurrent(generation)

            val target = try {
                resolver.resolve(request)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                return@withLock TuneOutcome.Failed(generation, TuneFailureStage.RESOLVE_TARGET)
            }
            ensureCurrent(generation)

            val lease = connections.tryAcquire(request.sourceId, request.role)
                ?: return@withLock TuneOutcome.Rejected(
                    generation,
                    TuneRejectionReason.SOURCE_CONNECTION_LIMIT,
                )

            val playback = try {
                opener.open(target)
            } catch (cancelled: CancellationException) {
                withContext(NonCancellable) { lease.release() }
                throw cancelled
            } catch (_: Exception) {
                withContext(NonCancellable) { lease.release() }
                return@withLock TuneOutcome.Failed(generation, TuneFailureStage.OPEN_PLAYBACK)
            }

            currentSession = CurrentSession(playback, lease)
            try {
                ensureCurrent(generation)
            } catch (cancelled: CancellationException) {
                withContext(NonCancellable) { closeCurrent(generation = null) }
                throw cancelled
            }

            TuneOutcome.Playing(
                generation = generation,
                sourceId = request.sourceId,
                role = request.role,
            )
        }
    }

    suspend fun stop(): StopOutcome = latest(
        superseded = StopOutcome::Superseded,
    ) { generation ->
        replacementMutex.withLock {
            ensureCurrent(generation)
            when (closeCurrent(generation)) {
                CloseResult.CLOSED -> StopOutcome.Stopped(generation)
                CloseResult.NOTHING_TO_CLOSE -> StopOutcome.AlreadyStopped(generation)
                CloseResult.FAILED -> StopOutcome.Failed(generation)
            }
        }
    }

    private suspend fun closeCurrent(generation: Long?): CloseResult {
        val owned = currentSession ?: return CloseResult.NOTHING_TO_CLOSE
        if (generation != null) ensureCurrent(generation)

        try {
            owned.playback.close()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return CloseResult.FAILED
        }

        if (generation != null) ensureCurrent(generation)
        owned.lease.release()
        currentSession = null
        return CloseResult.CLOSED
    }

    private suspend fun ensureCurrent(generation: Long) {
        currentCoroutineContext().ensureActive()
        generationMutex.withLock {
            if (generation != latestGeneration) throw SupersededTuneCancellation()
        }
    }

    private suspend fun <R> latest(
        superseded: (Long) -> R,
        block: suspend (Long) -> R,
    ): R = coroutineScope {
        var generation = 0L
        lateinit var operation: Deferred<R>
        operation = async(start = CoroutineStart.LAZY) { block(generation) }

        generationMutex.withLock {
            generation = ++latestGeneration
            activeOperation?.cancel(SupersededTuneCancellation())
            activeOperation = operation
        }
        operation.start()

        try {
            operation.await()
        } catch (_: SupersededTuneCancellation) {
            superseded(generation)
        } finally {
            withContext(NonCancellable) {
                generationMutex.withLock {
                    if (activeOperation === operation) activeOperation = null
                }
            }
        }
    }

    private data class CurrentSession(
        val playback: PlaybackSession,
        val lease: SourceConnectionLease,
    )

    private enum class CloseResult {
        CLOSED,
        NOTHING_TO_CLOSE,
        FAILED,
    }

    private class SupersededTuneCancellation : CancellationException("superseded tune generation")
}
