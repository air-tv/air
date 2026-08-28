package com.getair.core.auth

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant

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
        "DeviceAuthorization(deviceCode=<redacted>, userCode=$userCode, verificationUrl=<redacted>, " +
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
    private val now: () -> Instant = { Clock.System.now() },
) {
    private val commands = Mutex()
    private val mutableState = MutableStateFlow<TvAuthState>(TvAuthState.SignedOut)
    val state: StateFlow<TvAuthState> = mutableState.asStateFlow()

    suspend fun startDeviceAuthorization() = commands.withLock {
        val activeGateway = gateway ?: return setUnavailable()
        mutableState.value = TvAuthState.Authenticating
        mutableState.value = authTransition("Device authorization could not be started") {
            TvAuthState.AwaitingDeviceApproval(activeGateway.startDeviceAuthorization())
        }
    }

    suspend fun pollDeviceAuthorization() = commands.withLock {
        val activeGateway = gateway ?: return setUnavailable()
        val authorization = (state.value as? TvAuthState.AwaitingDeviceApproval)?.authorization ?: return
        if (now() >= authorization.expiresAt) {
            mutableState.value = TvAuthState.Failed("Device authorization expired")
            return
        }
        mutableState.value = authTransition("Device authorization could not be checked") {
            when (val result = activeGateway.pollDeviceAuthorization(authorization.deviceCode)) {
                DeviceAuthorizationResult.Pending -> TvAuthState.AwaitingDeviceApproval(authorization)
                is DeviceAuthorizationResult.Authenticated -> TvAuthState.SignedIn(result.account)
                DeviceAuthorizationResult.Denied -> TvAuthState.Failed("Device authorization was denied")
                DeviceAuthorizationResult.Expired -> TvAuthState.Failed("Device authorization expired")
            }
        }
    }

    suspend fun signIn(credentials: PasswordCredentials) = commands.withLock {
        val activeGateway = gateway ?: return setUnavailable()
        mutableState.value = TvAuthState.Authenticating
        mutableState.value = authTransition("Sign in failed") {
            TvAuthState.SignedIn(activeGateway.signIn(credentials))
        }
    }

    suspend fun signOut() = commands.withLock {
        try {
            gateway?.signOut()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            // Local sign-out still wins when a replaceable remote gateway is unavailable.
        }
        mutableState.value = TvAuthState.SignedOut
    }

    private suspend fun authTransition(
        failureMessage: String,
        block: suspend () -> TvAuthState,
    ): TvAuthState = try {
        block()
    } catch (error: CancellationException) {
        throw error
    } catch (_: Throwable) {
        TvAuthState.Failed(failureMessage)
    }

    private fun setUnavailable() {
        mutableState.value = TvAuthState.ServerUnavailable
    }
}
