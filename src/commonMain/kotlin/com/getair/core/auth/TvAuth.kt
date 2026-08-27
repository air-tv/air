package com.getair.core.auth

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.datetime.Instant

class PasswordCredentials(
    val username: String,
    val password: String,
) {
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
        require(userCode.isNotBlank())
        require(pollIntervalSeconds > 0)
    }

    override fun toString(): String =
        "DeviceAuthorization(deviceCode=<redacted>, userCode=$userCode, verificationUrl=<redacted>, " +
            "qrPayload=<redacted>, expiresAt=$expiresAt, pollIntervalSeconds=$pollIntervalSeconds)"
}

data class AuthenticatedAccount(
    val id: String,
    val displayName: String,
)

sealed interface DeviceAuthorizationResult {
    data object Pending : DeviceAuthorizationResult
    data class Authenticated(val account: AuthenticatedAccount) : DeviceAuthorizationResult
    data object Denied : DeviceAuthorizationResult
    data object Expired : DeviceAuthorizationResult
}

sealed interface TvAuthState {
    data object SignedOut : TvAuthState
    data object ServerUnavailable : TvAuthState
    data object Authenticating : TvAuthState
    data class AwaitingDeviceApproval(val authorization: DeviceAuthorization) : TvAuthState
    data class SignedIn(val account: AuthenticatedAccount) : TvAuthState
    data class Failed(val message: String) : TvAuthState
}

interface TvAuthGateway {
    suspend fun startDeviceAuthorization(): DeviceAuthorization
    suspend fun pollDeviceAuthorization(deviceCode: String): DeviceAuthorizationResult
    suspend fun signIn(credentials: PasswordCredentials): AuthenticatedAccount
    suspend fun signOut()
}

class TvAuthController(
    private val gateway: TvAuthGateway? = null,
) {
    private val mutableState = MutableStateFlow<TvAuthState>(TvAuthState.SignedOut)
    val state: StateFlow<TvAuthState> = mutableState.asStateFlow()

    suspend fun startDeviceAuthorization() {
        val activeGateway = gateway ?: return setUnavailable()
        mutableState.value = TvAuthState.Authenticating
        mutableState.value = try {
            TvAuthState.AwaitingDeviceApproval(activeGateway.startDeviceAuthorization())
        } catch (_: Throwable) {
            TvAuthState.Failed("Device authorization could not be started")
        }
    }

    suspend fun pollDeviceAuthorization() {
        val activeGateway = gateway ?: return setUnavailable()
        val authorization = (state.value as? TvAuthState.AwaitingDeviceApproval)?.authorization ?: return
        mutableState.value = try {
            when (val result = activeGateway.pollDeviceAuthorization(authorization.deviceCode)) {
                DeviceAuthorizationResult.Pending -> TvAuthState.AwaitingDeviceApproval(authorization)
                is DeviceAuthorizationResult.Authenticated -> TvAuthState.SignedIn(result.account)
                DeviceAuthorizationResult.Denied -> TvAuthState.Failed("Device authorization was denied")
                DeviceAuthorizationResult.Expired -> TvAuthState.Failed("Device authorization expired")
            }
        } catch (_: Throwable) {
            TvAuthState.Failed("Device authorization could not be checked")
        }
    }

    suspend fun signIn(credentials: PasswordCredentials) {
        val activeGateway = gateway ?: return setUnavailable()
        mutableState.value = TvAuthState.Authenticating
        mutableState.value = try {
            TvAuthState.SignedIn(activeGateway.signIn(credentials))
        } catch (_: Throwable) {
            TvAuthState.Failed("Sign in failed")
        }
    }

    suspend fun signOut() {
        gateway?.let { runCatching { it.signOut() } }
        mutableState.value = TvAuthState.SignedOut
    }

    private fun setUnavailable() {
        mutableState.value = TvAuthState.ServerUnavailable
    }
}
