package com.getair.core.auth

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.datetime.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class TvAuthTest {
    @Test
    fun noServerRemainsAValidLocalOnlyState() = runTest {
        val controller = controller()

        controller.startDeviceAuthorization()
        assertIs<TvAuthState.ServerUnavailable>(controller.state.value)

        controller.signIn(PasswordCredentials("person@example.invalid", "password-secret"))
        assertIs<TvAuthState.ServerUnavailable>(controller.state.value)
    }

    @Test
    fun secretBearingModelsNeverRenderTheirSecrets() {
        val authorization = authorization()
        val credentials = PasswordCredentials("person@example.invalid", "password-secret")
        val account = AuthenticatedAccount("account-secret", "Living Room")

        val renderedAuthorization = authorization.toString()
        assertFalse("device-secret" in renderedAuthorization)
        assertFalse("AIR-SECRET" in renderedAuthorization)
        assertFalse("https://login.invalid/private" in renderedAuthorization)
        assertFalse("qr-secret" in renderedAuthorization)
        assertFalse("person@example.invalid" in credentials.toString())
        assertFalse("password-secret" in credentials.toString())
        assertFalse("account-secret" in account.toString())
        assertFalse("device-secret" in TvAuthState.AwaitingDeviceApproval(authorization).toString())
        assertFalse("account-secret" in TvAuthState.SignedIn(account).toString())
    }

    @Test
    fun automaticPollingHonorsServerIntervalAndManualNudgeCannotPollEarly() = runTest {
        val gateway = FakeGateway(
            poll = { DeviceAuthorizationResult.Authenticated(account("automatic")) },
        )
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        controller.pollDeviceAuthorization()
        assertEquals(0, gateway.pollCount)
        advanceTimeBy(4_999)
        runCurrent()
        assertEquals(0, gateway.pollCount)

        advanceTimeBy(1)
        runCurrent()
        assertEquals(1, gateway.pollCount)
        assertEquals("automatic", assertIs<TvAuthState.SignedIn>(controller.state.value).account.id)
    }

    @Test
    fun expiryStopsWithoutPollingWhenIntervalFallsBeyondLifetime() = runTest {
        val gateway = FakeGateway(
            startAuthorization = { authorization(expiresAfterSeconds = 5, pollIntervalSeconds = 10) },
        )
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        advanceTimeBy(5_000)
        runCurrent()

        assertEquals(0, gateway.pollCount)
        assertEquals(
            TvAuthFailure.DeviceAuthorizationExpired,
            assertIs<TvAuthState.Failed>(controller.state.value).reason,
        )
    }

    @Test
    fun slowDownPermanentlyRaisesCadenceAndHonorsLongerServerDelay() = runTest {
        val results = scriptedResults(
            DeviceAuthorizationResult.SlowDown(retryAfterSeconds = 12),
            DeviceAuthorizationResult.Authenticated(account("approved")),
        )
        val gateway = FakeGateway(poll = { results() })
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        advanceTimeBy(5.seconds)
        runCurrent()
        val waiting = assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value)
        assertEquals(12, assertIs<DevicePollingState.SlowedDown>(waiting.polling).nextPollInSeconds)

        advanceTimeBy(11_999)
        runCurrent()
        assertEquals(1, gateway.pollCount)
        advanceTimeBy(1)
        runCurrent()
        assertEquals(2, gateway.pollCount)
        assertIs<TvAuthState.SignedIn>(controller.state.value)
    }

    @Test
    fun retryAfterUsesTheExactServerFloorWithoutChangingBaseCadence() = runTest {
        val results = scriptedResults(
            DeviceAuthorizationResult.RetryAfter(9),
            DeviceAuthorizationResult.Pending,
            DeviceAuthorizationResult.Authenticated(account("approved")),
        )
        val controller = controller(FakeGateway(poll = { results() }))
        controller.startDeviceAuthorization()

        advanceTimeBy(5.seconds)
        runCurrent()
        assertEquals(
            9,
            assertIs<DevicePollingState.ServerRetryAfter>(
                assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value).polling,
            ).nextPollInSeconds,
        )

        advanceTimeBy(9.seconds)
        runCurrent()
        assertIs<DevicePollingState.Waiting>(
            assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value).polling,
        ).also { assertEquals(5, it.nextPollInSeconds) }
        advanceTimeBy(5.seconds)
        runCurrent()
        assertIs<TvAuthState.SignedIn>(controller.state.value)
    }

    @Test
    fun transientFailuresRecoverWithBoundedBackoffAndNoErrorDetails() = runTest {
        var invocation = 0
        val gateway = FakeGateway(
            poll = {
                when (invocation++) {
                    0 -> DeviceAuthorizationResult.TransientNetworkFailure()
                    1 -> error("https://private.invalid/device-secret")
                    else -> DeviceAuthorizationResult.Authenticated(account("recovered"))
                }
            },
        )
        val controller = controller(
            gateway,
            TvAuthPollingPolicy(
                maxConsecutiveTransientFailures = 3,
                initialTransientBackoffSeconds = 1,
                maxTransientBackoffSeconds = 2,
            ),
        )
        controller.startDeviceAuthorization()

        advanceTimeBy(5.seconds)
        runCurrent()
        assertEquals(
            DevicePollingState.Recovering(attempt = 1, nextPollInSeconds = 1),
            assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value).polling,
        )
        advanceTimeBy(1.seconds)
        runCurrent()
        val recovering = assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value)
        assertEquals(DevicePollingState.Recovering(2, 2), recovering.polling)
        assertFalse("private.invalid" in recovering.toString())
        assertFalse("device-secret" in recovering.toString())

        advanceTimeBy(2.seconds)
        runCurrent()
        assertEquals("recovered", assertIs<TvAuthState.SignedIn>(controller.state.value).account.id)
    }

    @Test
    fun transientRetryBudgetTerminatesInsteadOfLoopingForever() = runTest {
        val gateway = FakeGateway(
            poll = { DeviceAuthorizationResult.TransientNetworkFailure() },
        )
        val controller = controller(
            gateway,
            TvAuthPollingPolicy(
                maxConsecutiveTransientFailures = 2,
                initialTransientBackoffSeconds = 1,
                maxTransientBackoffSeconds = 2,
            ),
        )
        controller.startDeviceAuthorization()

        advanceTimeBy(5.seconds)
        runCurrent()
        advanceTimeBy(1.seconds)
        runCurrent()
        advanceTimeBy(2.seconds)
        runCurrent()

        assertEquals(3, gateway.pollCount)
        assertEquals(
            TvAuthFailure.DeviceAuthorizationNetworkUnavailable,
            assertIs<TvAuthState.Failed>(controller.state.value).reason,
        )
        advanceTimeBy(60.seconds)
        runCurrent()
        assertEquals(3, gateway.pollCount)
    }

    @Test
    fun replacementCancelsOldAttemptAndRejectsItsNonCancellableStaleCompletion() = runTest {
        var authorizationNumber = 0
        val oldPollStarted = CompletableDeferred<Unit>()
        val releaseOldPoll = CompletableDeferred<Unit>()
        val gateway = FakeGateway(
            startAuthorization = {
                authorizationNumber++
                authorization(deviceCode = "device-$authorizationNumber")
            },
            poll = { deviceCode ->
                if (deviceCode == "device-1") {
                    oldPollStarted.complete(Unit)
                    withContext(NonCancellable) { releaseOldPoll.await() }
                    DeviceAuthorizationResult.Authenticated(account("stale"))
                } else {
                    DeviceAuthorizationResult.Authenticated(account("current"))
                }
            },
        )
        val controller = controller(gateway)
        controller.startDeviceAuthorization()
        advanceTimeBy(5.seconds)
        runCurrent()
        oldPollStarted.await()

        controller.startDeviceAuthorization()
        releaseOldPoll.complete(Unit)
        runCurrent()
        assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value)

        advanceTimeBy(5.seconds)
        runCurrent()
        assertEquals("current", assertIs<TvAuthState.SignedIn>(controller.state.value).account.id)
        assertEquals(listOf("device-1", "device-2"), gateway.polledDeviceCodes)
    }

    @Test
    fun explicitCancellationStopsPollingAndReturnsToLocalSignedOutState() = runTest {
        val gateway = FakeGateway(poll = { DeviceAuthorizationResult.Pending })
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        controller.cancelAuthentication()
        advanceTimeBy(60.seconds)
        runCurrent()

        assertIs<TvAuthState.SignedOut>(controller.state.value)
        assertEquals(0, gateway.pollCount)
    }

    @Test
    fun closeCancelsTheLifecycleAndRejectsFutureAttempts() = runTest {
        val gateway = FakeGateway(poll = { DeviceAuthorizationResult.Pending })
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        controller.close()
        controller.startDeviceAuthorization()
        advanceTimeBy(60.seconds)
        runCurrent()

        assertIs<TvAuthState.SignedOut>(controller.state.value)
        assertEquals(0, gateway.pollCount)
    }

    @Test
    fun callerCancellationCancelsAStartingGatewayRequestWithoutLeavingAuthenticationStuck() = runTest {
        var cancelled = false
        val gateway = FakeGateway(
            startAuthorization = {
                try {
                    awaitCancellation()
                } finally {
                    cancelled = true
                }
            },
        )
        val controller = controller(gateway)
        val caller = launch { controller.startDeviceAuthorization() }
        runCurrent()
        assertIs<TvAuthState.Authenticating>(controller.state.value)

        caller.cancelAndJoin()
        runCurrent()

        assertTrue(cancelled)
        assertIs<TvAuthState.SignedOut>(controller.state.value)
    }

    @Test
    fun denialIsTerminalAndNeverPollsAgain() = runTest {
        val gateway = FakeGateway(poll = { DeviceAuthorizationResult.Denied })
        val controller = controller(gateway)
        controller.startDeviceAuthorization()

        advanceTimeBy(5.seconds)
        runCurrent()
        assertEquals(
            TvAuthFailure.DeviceAuthorizationDenied,
            assertIs<TvAuthState.Failed>(controller.state.value).reason,
        )
        advanceTimeBy(60.seconds)
        runCurrent()
        assertEquals(1, gateway.pollCount)
    }

    @Test
    fun passwordSignInUsesGatewayWithoutRetainingCredentialsInState() = runTest {
        var receivedUsername: String? = null
        var receivedPassword: String? = null
        val gateway = FakeGateway(
            passwordSignIn = {
                receivedUsername = it.username
                receivedPassword = it.password
                account("password-account")
            },
        )
        val controller = controller(gateway)

        controller.signIn(PasswordCredentials("person@example.invalid", "password-secret"))

        assertEquals("person@example.invalid", receivedUsername)
        assertEquals("password-secret", receivedPassword)
        val state = assertIs<TvAuthState.SignedIn>(controller.state.value)
        assertEquals("password-account", state.account.id)
        assertFalse("person@example.invalid" in state.toString())
        assertFalse("password-secret" in state.toString())
    }

    @Test
    fun localSignOutWinsWhenRemoteCleanupFails() = runTest {
        val gateway = FakeGateway(
            passwordSignIn = { account("signed-in") },
            remoteSignOut = { error("remote failure") },
        )
        val controller = controller(gateway)
        controller.signIn(PasswordCredentials("person", "secret"))

        controller.signOut()

        assertIs<TvAuthState.SignedOut>(controller.state.value)
        assertEquals(1, gateway.signOutCount)
    }

    private fun TestScope.controller(
        gateway: TvAuthGateway? = null,
        policy: TvAuthPollingPolicy = TvAuthPollingPolicy(),
    ): TvAuthController = TvAuthController(
        gateway = gateway,
        coroutineScope = backgroundScope,
        pollingPolicy = policy,
        now = { BASE_TIME + testScheduler.currentTime.milliseconds },
    )

    private class FakeGateway(
        private val startAuthorization: suspend () -> DeviceAuthorization = { authorization() },
        private val poll: suspend (String) -> DeviceAuthorizationResult = { DeviceAuthorizationResult.Pending },
        private val passwordSignIn: suspend (PasswordCredentials) -> AuthenticatedAccount = { account("account-1") },
        private val remoteSignOut: suspend () -> Unit = {},
    ) : TvAuthGateway {
        val polledDeviceCodes = mutableListOf<String>()
        var signOutCount = 0
            private set
        val pollCount: Int get() = polledDeviceCodes.size

        override suspend fun startDeviceAuthorization(): DeviceAuthorization = startAuthorization()

        override suspend fun pollDeviceAuthorization(deviceCode: String): DeviceAuthorizationResult {
            polledDeviceCodes += deviceCode
            return poll(deviceCode)
        }

        override suspend fun signIn(credentials: PasswordCredentials): AuthenticatedAccount =
            passwordSignIn(credentials)

        override suspend fun signOut() {
            signOutCount++
            remoteSignOut()
        }
    }

    companion object {
        private val BASE_TIME = Instant.parse("2026-08-28T12:00:00Z")

        private fun authorization(
            deviceCode: String = "device-secret",
            expiresAfterSeconds: Int = 120,
            pollIntervalSeconds: Int = 5,
        ): DeviceAuthorization = DeviceAuthorization(
            deviceCode = deviceCode,
            userCode = "AIR-SECRET",
            verificationUrl = "https://login.invalid/private",
            qrPayload = "qr-secret",
            expiresAt = BASE_TIME + expiresAfterSeconds.seconds,
            pollIntervalSeconds = pollIntervalSeconds,
        )

        private fun account(id: String): AuthenticatedAccount =
            AuthenticatedAccount(id, "Living Room")

        private fun scriptedResults(
            vararg results: DeviceAuthorizationResult,
        ): suspend () -> DeviceAuthorizationResult {
            var index = 0
            return {
                check(index < results.size) { "No scripted authentication result remains" }
                results[index++]
            }
        }
    }
}
