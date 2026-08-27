package com.getair.core.auth

import kotlinx.coroutines.test.runTest
import kotlinx.datetime.Instant
import kotlin.test.Test
import kotlin.test.assertFalse
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
        val controller = TvAuthController(gateway)
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

    private class FakeGateway : TvAuthGateway {
        override suspend fun startDeviceAuthorization(): DeviceAuthorization = DeviceAuthorization(
            deviceCode = "device-secret",
            userCode = "AIR-2026",
            verificationUrl = "https://login.invalid/device",
            qrPayload = "qr-secret",
            expiresAt = Instant.parse("2026-08-27T12:00:00Z"),
            pollIntervalSeconds = 5,
        )

        override suspend fun pollDeviceAuthorization(deviceCode: String): DeviceAuthorizationResult =
            DeviceAuthorizationResult.Authenticated(AuthenticatedAccount("account-1", "Living Room"))

        override suspend fun signIn(credentials: PasswordCredentials): AuthenticatedAccount =
            AuthenticatedAccount("account-1", "Living Room")

        override suspend fun signOut() = Unit
    }
}
