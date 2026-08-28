package com.getair.core.auth

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Instant
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class TvAuthTest {
    @Test
    fun noServerRemainsAValidLocalOnlyState() = runTest {
        val controller = TvAuthController()
        controller.startDeviceAuthorization()
        assertIs<TvAuthState.ServerUnavailable>(controller.state.value)
    }

    @Test
    fun supportsDeviceCodeAndPasswordWithoutRenderingSecrets() = runTest {
        val gateway = FakeGateway()
        val controller = TvAuthController(gateway) { Instant.parse("2026-08-27T11:59:00Z") }
        controller.startDeviceAuthorization()
        val awaiting = assertIs<TvAuthState.AwaitingDeviceApproval>(controller.state.value)
        assertFalse("device-secret" in awaiting.authorization.toString())
        assertFalse("qr-secret" in awaiting.authorization.toString())

        controller.pollDeviceAuthorization()
        assertIs<TvAuthState.SignedIn>(controller.state.value)

        val password = PasswordCredentials("person@example.invalid", "password-secret")
        assertFalse("person@example.invalid" in password.toString())
        assertFalse("password-secret" in password.toString())
        controller.signIn(password)
        assertIs<TvAuthState.SignedIn>(controller.state.value)
    }

    @Test
    fun expiredDeviceCodeIsRejectedWithoutPollingTheGateway() = runTest {
        val gateway = FakeGateway()
        val controller = TvAuthController(gateway) { Instant.parse("2026-08-27T12:00:00Z") }
        controller.startDeviceAuthorization()

        controller.pollDeviceAuthorization()

        assertIs<TvAuthState.Failed>(controller.state.value)
        assertFalse(gateway.polled)
    }

    @Test
    fun coroutineCancellationIsNeverConvertedIntoAnAuthFailure() = runTest {
        val gateway = FakeGateway(cancelStart = true)
        val controller = TvAuthController(gateway)

        assertFailsWith<CancellationException> { controller.startDeviceAuthorization() }
        assertIs<TvAuthState.Authenticating>(controller.state.value)
    }

    private class FakeGateway(
        private val cancelStart: Boolean = false,
    ) : TvAuthGateway {
        var polled = false

        override suspend fun startDeviceAuthorization(): DeviceAuthorization = DeviceAuthorization(
            deviceCode = "device-secret",
            userCode = "AIR-2026",
            verificationUrl = "https://login.invalid/device",
            qrPayload = "qr-secret",
            expiresAt = Instant.parse("2026-08-27T12:00:00Z"),
            pollIntervalSeconds = 5,
        ).also { if (cancelStart) throw CancellationException("cancel test") }

        override suspend fun pollDeviceAuthorization(deviceCode: String): DeviceAuthorizationResult {
            polled = true
            return DeviceAuthorizationResult.Authenticated(AuthenticatedAccount("account-1", "Living Room"))
        }

        override suspend fun signIn(credentials: PasswordCredentials): AuthenticatedAccount =
            AuthenticatedAccount("account-1", "Living Room")

        override suspend fun signOut() = Unit
    }
}
