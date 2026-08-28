package com.getair.core.auth

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class PasswordCredentials(
    val username: String,
    val password: String,
) {
    init {
        require(username.isNotBlank())
        require(password.isNotEmpty())
    }

    override fun toString(): String = "PasswordCredentials(username=<redacted>, password=<redacted>)"
}

class DeviceAuthorization(
    val deviceCode: String,
    val userCode: String,
    val verificationUrl: String,
    val qrPayload: String,
    val expiresAt: Instant,
    val pollIntervalSeconds: Int,
) {
    init {
        require(deviceCode.isNotBlank())
        require(userCode.isNotBlank())
        require(verificationUrl.isNotBlank())
        require(qrPayload.isNotBlank())
        require(pollIntervalSeconds > 0)
    }

    override fun toString(): String =
        "DeviceAuthorization(deviceCode=<redacted>, userCode=<redacted>, verificationUrl=<redacted>, " +
            "qrPayload=<redacted>, expiresAt=$expiresAt, pollIntervalSeconds=$pollIntervalSeconds)"
}

data class AuthenticatedAccount(
    val id: String,
    val displayName: String,
) {
    init {
        require(id.isNotBlank())
        require(displayName.isNotBlank())
    }

    override fun toString(): String = "AuthenticatedAccount(id=<redacted>, displayName=$displayName)"
}

/** Results are already protocol-classified by the gateway; no transport exception crosses this API. */
sealed interface DeviceAuthorizationResult {
    data object Pending : DeviceAuthorizationResult
    data class SlowDown(val retryAfterSeconds: Int? = null) : DeviceAuthorizationResult {
        init {
            require(retryAfterSeconds == null || retryAfterSeconds > 0)
        }
    }

    data class RetryAfter(val retryAfterSeconds: Int) : DeviceAuthorizationResult {
        init {
            require(retryAfterSeconds > 0)
        }
    }

    data class TransientNetworkFailure(val retryAfterSeconds: Int? = null) : DeviceAuthorizationResult {
        init {
            require(retryAfterSeconds == null || retryAfterSeconds > 0)
        }
    }

    data class Authenticated(val account: AuthenticatedAccount) : DeviceAuthorizationResult
    data object Denied : DeviceAuthorizationResult
    data object Expired : DeviceAuthorizationResult
}

sealed interface DevicePollingState {
    val nextPollInSeconds: Int

    data class Waiting(override val nextPollInSeconds: Int) : DevicePollingState {
        init {
            require(nextPollInSeconds > 0)
        }
    }

    data class SlowedDown(override val nextPollInSeconds: Int) : DevicePollingState {
        init {
            require(nextPollInSeconds > 0)
        }
    }

    data class ServerRetryAfter(override val nextPollInSeconds: Int) : DevicePollingState {
        init {
            require(nextPollInSeconds > 0)
        }
    }

    data class Recovering(
        val attempt: Int,
        override val nextPollInSeconds: Int,
    ) : DevicePollingState {
        init {
            require(attempt > 0)
            require(nextPollInSeconds > 0)
        }
    }
}

enum class TvAuthFailure(val message: String) {
    DeviceAuthorizationStartFailed("Device authorization could not be started"),
    DeviceAuthorizationExpired("Device authorization expired"),
    DeviceAuthorizationDenied("Device authorization was denied"),
    DeviceAuthorizationNetworkUnavailable("Device authorization is temporarily unavailable"),
    PasswordSignInFailed("Sign in failed"),
}

sealed interface TvAuthState {
    data object SignedOut : TvAuthState
    data object ServerUnavailable : TvAuthState
    data object Authenticating : TvAuthState
    data class AwaitingDeviceApproval(
        val authorization: DeviceAuthorization,
        val polling: DevicePollingState = DevicePollingState.Waiting(authorization.pollIntervalSeconds),
    ) : TvAuthState

    data class SignedIn(val account: AuthenticatedAccount) : TvAuthState
    data class Failed(val reason: TvAuthFailure) : TvAuthState {
        val message: String get() = reason.message
    }
}

/**
 * Owns remote protocol calls and any access/refresh tokens in its injected secure store.
 * Controller models deliberately contain no tokens.
 */
interface TvAuthGateway {
    suspend fun startDeviceAuthorization(): DeviceAuthorization
    suspend fun pollDeviceAuthorization(deviceCode: String): DeviceAuthorizationResult
    suspend fun signIn(credentials: PasswordCredentials): AuthenticatedAccount
    suspend fun signOut()
}

data class TvAuthPollingPolicy(
    val slowDownIncrementSeconds: Int = 5,
    val maxConsecutiveTransientFailures: Int = 5,
    val initialTransientBackoffSeconds: Int = 1,
    val maxTransientBackoffSeconds: Int = 30,
) {
    init {
        require(slowDownIncrementSeconds > 0)
        require(maxConsecutiveTransientFailures >= 0)
        require(initialTransientBackoffSeconds > 0)
        require(maxTransientBackoffSeconds >= initialTransientBackoffSeconds)
    }
}

class TvAuthController(
    private val gateway: TvAuthGateway? = null,
    coroutineScope: CoroutineScope? = null,
    private val pollingPolicy: TvAuthPollingPolicy = TvAuthPollingPolicy(),
    private val now: () -> Instant = { Clock.System.now() },
) {
    private val lifecycleJob = SupervisorJob(coroutineScope?.coroutineContext?.get(Job))
    private val workerScope = CoroutineScope(
        (coroutineScope?.coroutineContext ?: Dispatchers.Default) + lifecycleJob,
    )
    private val commands = Mutex()
    private val mutableState = MutableStateFlow<TvAuthState>(TvAuthState.SignedOut)
    private var generation = 0L
    private var activeJob: Job? = null
    private var activeAttempt: DeviceAttempt? = null
    private var closed = false

    val state: StateFlow<TvAuthState> = mutableState.asStateFlow()

    /** Starts device authorization and then polls automatically at the server-provided cadence. */
    suspend fun startDeviceAuthorization() {
        val activeGateway = gateway ?: return replaceWith(TvAuthState.ServerUnavailable)
        val ticket = commands.withLock {
            if (closed) return
            val previous = activeJob
            val nextGeneration = ++generation
            val request = workerScope.async(start = CoroutineStart.LAZY) {
                activeGateway.startDeviceAuthorization()
            }
            activeAttempt = null
            activeJob = request
            mutableState.value = TvAuthState.Authenticating
            StartTicket(nextGeneration, previous, request)
        }
        ticket.previous?.cancel()
        ticket.request.start()

        val authorization = try {
            ticket.request.await()
        } catch (error: CancellationException) {
            ticket.request.cancel()
            withContext(NonCancellable) { clearCancelledStart(ticket.generation) }
            throw error
        } catch (_: Throwable) {
            failStart(ticket.generation)
            return
        }

        val pollingJob = try {
            commands.withLock {
                if (closed || generation != ticket.generation || activeJob !== ticket.request) return@withLock null
                if (now() >= authorization.expiresAt) {
                    activeJob = null
                    mutableState.value = TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationExpired)
                    return@withLock null
                }

                val attemptJob = SupervisorJob(lifecycleJob)
                val attempt = DeviceAttempt(
                    generation = ticket.generation,
                    authorization = authorization,
                    job = attemptJob,
                    intervalSeconds = authorization.pollIntervalSeconds,
                    nextPollAt = now() + authorization.pollIntervalSeconds.seconds,
                )
                activeAttempt = attempt
                activeJob = attemptJob
                mutableState.value = TvAuthState.AwaitingDeviceApproval(authorization)
                CoroutineScope(workerScope.coroutineContext + attemptJob).launch(start = CoroutineStart.LAZY) {
                    try {
                        pollAutomatically(attempt, activeGateway)
                    } catch (error: CancellationException) {
                        throw error
                    } catch (_: Throwable) {
                        withContext(NonCancellable) {
                            finish(
                                attempt,
                                TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationNetworkUnavailable),
                            )
                        }
                    }
                }
            }
        } catch (error: CancellationException) {
            withContext(NonCancellable) { clearCancelledStart(ticket.generation) }
            throw error
        }
        pollingJob?.start()
    }

    /**
     * Compatibility nudge for callers that previously drove polling manually. It never polls before
     * the server's due time and all actual work remains a child of the active attempt.
     */
    suspend fun pollDeviceAuthorization() {
        val activeGateway = gateway ?: return replaceWith(TvAuthState.ServerUnavailable)
        val attempt = commands.withLock { activeAttempt } ?: return
        val request = CoroutineScope(workerScope.coroutineContext + attempt.job).async {
            pollIfDue(attempt, activeGateway)
        }
        try {
            request.await()
        } catch (error: CancellationException) {
            request.cancel()
            throw error
        }
    }

    suspend fun signIn(credentials: PasswordCredentials) {
        val activeGateway = gateway ?: return replaceWith(TvAuthState.ServerUnavailable)
        val ticket = commands.withLock {
            if (closed) return
            val previous = activeJob
            val nextGeneration = ++generation
            val request = workerScope.async(start = CoroutineStart.LAZY) {
                activeGateway.signIn(credentials)
            }
            activeAttempt = null
            activeJob = request
            mutableState.value = TvAuthState.Authenticating
            PasswordTicket(nextGeneration, previous, request)
        }
        ticket.previous?.cancel()
        ticket.request.start()

        val account = try {
            ticket.request.await()
        } catch (error: CancellationException) {
            ticket.request.cancel()
            withContext(NonCancellable) { clearCancelledStart(ticket.generation) }
            throw error
        } catch (_: Throwable) {
            commands.withLock {
                if (generation == ticket.generation && activeJob === ticket.request) {
                    activeJob = null
                    mutableState.value = TvAuthState.Failed(TvAuthFailure.PasswordSignInFailed)
                }
            }
            return
        }

        try {
            commands.withLock {
                if (!closed && generation == ticket.generation && activeJob === ticket.request) {
                    activeJob = null
                    mutableState.value = TvAuthState.SignedIn(account)
                }
            }
        } catch (error: CancellationException) {
            withContext(NonCancellable) { clearCancelledStart(ticket.generation) }
            throw error
        }
    }

    /** Cancels only the in-flight authentication attempt; it performs no remote sign-out. */
    suspend fun cancelAuthentication() {
        val previous = commands.withLock {
            if (closed) return
            ++generation
            activeAttempt = null
            activeJob.also {
                activeJob = null
                mutableState.value = TvAuthState.SignedOut
            }
        }
        previous?.cancel()
    }

    /** Local sign-out is committed before the optional remote cleanup call and cannot be rolled back. */
    suspend fun signOut() {
        val previous = commands.withLock {
            if (closed) return
            ++generation
            activeAttempt = null
            activeJob.also {
                activeJob = null
                mutableState.value = TvAuthState.SignedOut
            }
        }
        previous?.cancel()
        try {
            gateway?.signOut()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            // The replaceable remote cleanup is best-effort; local sign-out already won.
        }
    }

    /** Ends the controller lifecycle and rejects all stale completions. */
    suspend fun close() {
        val previous = commands.withLock {
            if (closed) return
            closed = true
            ++generation
            activeAttempt = null
            activeJob.also {
                activeJob = null
                mutableState.value = TvAuthState.SignedOut
            }
        }
        previous?.cancel()
        lifecycleJob.cancel()
    }

    private suspend fun pollAutomatically(attempt: DeviceAttempt, activeGateway: TvAuthGateway) {
        while (isCurrent(attempt)) {
            val waitMillis = attempt.polling.withLock {
                minOf(
                    millisecondsUntil(attempt.nextPollAt),
                    millisecondsUntil(attempt.authorization.expiresAt),
                )
            }
            if (waitMillis > 0) delay(waitMillis.milliseconds)
            if (!pollIfDue(attempt, activeGateway)) return
        }
    }

    private suspend fun pollIfDue(attempt: DeviceAttempt, activeGateway: TvAuthGateway): Boolean =
        attempt.polling.withLock {
            if (!isCurrent(attempt)) return@withLock false
            val pollTime = now()
            if (pollTime >= attempt.authorization.expiresAt) {
                finish(attempt, TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationExpired))
                return@withLock false
            }
            if (pollTime < attempt.nextPollAt) return@withLock true

            val result = try {
                activeGateway.pollDeviceAuthorization(attempt.authorization.deviceCode)
            } catch (error: CancellationException) {
                throw error
            } catch (_: Throwable) {
                DeviceAuthorizationResult.TransientNetworkFailure()
            }
            if (!isCurrent(attempt)) return@withLock false

            when (result) {
                DeviceAuthorizationResult.Pending -> {
                    attempt.transientFailures = 0
                    schedule(attempt, attempt.intervalSeconds, DevicePollingState.Waiting(attempt.intervalSeconds))
                }

                is DeviceAuthorizationResult.SlowDown -> {
                    attempt.transientFailures = 0
                    val increased = saturatedAdd(attempt.intervalSeconds, pollingPolicy.slowDownIncrementSeconds)
                    attempt.intervalSeconds = max(increased, result.retryAfterSeconds ?: increased)
                    schedule(
                        attempt,
                        attempt.intervalSeconds,
                        DevicePollingState.SlowedDown(attempt.intervalSeconds),
                    )
                }

                is DeviceAuthorizationResult.RetryAfter -> {
                    attempt.transientFailures = 0
                    schedule(
                        attempt,
                        result.retryAfterSeconds,
                        DevicePollingState.ServerRetryAfter(result.retryAfterSeconds),
                    )
                }

                is DeviceAuthorizationResult.TransientNetworkFailure -> {
                    val failureCount = ++attempt.transientFailures
                    if (failureCount > pollingPolicy.maxConsecutiveTransientFailures) {
                        finish(
                            attempt,
                            TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationNetworkUnavailable),
                        )
                        false
                    } else {
                        val retrySeconds = result.retryAfterSeconds ?: transientBackoff(failureCount)
                        schedule(
                            attempt,
                            retrySeconds,
                            DevicePollingState.Recovering(failureCount, retrySeconds),
                        )
                    }
                }

                is DeviceAuthorizationResult.Authenticated -> {
                    finish(attempt, TvAuthState.SignedIn(result.account))
                    false
                }

                DeviceAuthorizationResult.Denied -> {
                    finish(attempt, TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationDenied))
                    false
                }

                DeviceAuthorizationResult.Expired -> {
                    finish(attempt, TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationExpired))
                    false
                }
            }
        }

    private suspend fun schedule(
        attempt: DeviceAttempt,
        delaySeconds: Int,
        pollingState: DevicePollingState,
    ): Boolean {
        attempt.nextPollAt = now() + delaySeconds.seconds
        commands.withLock {
            if (!isCurrentLocked(attempt)) return false
            mutableState.value = TvAuthState.AwaitingDeviceApproval(attempt.authorization, pollingState)
        }
        return true
    }

    private suspend fun finish(attempt: DeviceAttempt, terminalState: TvAuthState) {
        commands.withLock {
            if (!isCurrentLocked(attempt)) return
            activeAttempt = null
            activeJob = null
            mutableState.value = terminalState
        }
        attempt.job.complete()
    }

    private suspend fun isCurrent(attempt: DeviceAttempt): Boolean =
        commands.withLock { isCurrentLocked(attempt) }

    private fun isCurrentLocked(attempt: DeviceAttempt): Boolean =
        !closed && generation == attempt.generation && activeAttempt === attempt && activeJob === attempt.job

    private suspend fun clearCancelledStart(cancelledGeneration: Long) {
        commands.withLock {
            if (!closed && generation == cancelledGeneration) {
                ++generation
                activeJob = null
                activeAttempt = null
                mutableState.value = TvAuthState.SignedOut
            }
        }
    }

    private suspend fun failStart(failedGeneration: Long) {
        commands.withLock {
            if (!closed && generation == failedGeneration) {
                activeJob = null
                mutableState.value = TvAuthState.Failed(TvAuthFailure.DeviceAuthorizationStartFailed)
            }
        }
    }

    private suspend fun replaceWith(nextState: TvAuthState) {
        val previous = commands.withLock {
            if (closed) return
            ++generation
            activeAttempt = null
            activeJob.also {
                activeJob = null
                mutableState.value = nextState
            }
        }
        previous?.cancel()
    }

    private fun millisecondsUntil(target: Instant): Long =
        (target - now()).inWholeMilliseconds.coerceAtLeast(0)

    private fun transientBackoff(attempt: Int): Int {
        var delaySeconds = pollingPolicy.initialTransientBackoffSeconds
        var remainingDoublings = attempt - 1
        while (remainingDoublings > 0 && delaySeconds < pollingPolicy.maxTransientBackoffSeconds) {
            delaySeconds = saturatedAdd(delaySeconds, delaySeconds)
                .coerceAtMost(pollingPolicy.maxTransientBackoffSeconds)
            remainingDoublings--
        }
        return delaySeconds.coerceAtMost(pollingPolicy.maxTransientBackoffSeconds)
    }

    private fun saturatedAdd(left: Int, right: Int): Int =
        (left.toLong() + right.toLong()).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

    private data class StartTicket(
        val generation: Long,
        val previous: Job?,
        val request: kotlinx.coroutines.Deferred<DeviceAuthorization>,
    )

    private data class PasswordTicket(
        val generation: Long,
        val previous: Job?,
        val request: kotlinx.coroutines.Deferred<AuthenticatedAccount>,
    )

    private class DeviceAttempt(
        val generation: Long,
        val authorization: DeviceAuthorization,
        val job: CompletableJob,
        var intervalSeconds: Int,
        var nextPollAt: Instant,
    ) {
        val polling = Mutex()
        var transientFailures: Int = 0
    }
}
