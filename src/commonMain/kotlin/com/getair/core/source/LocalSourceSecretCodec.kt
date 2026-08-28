package com.getair.core.source

import com.getair.iptv.StalkerCredentials
import com.getair.iptv.XtreamCredentials
import com.getair.iptv.model.StreamFormat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private const val VAULT_FORMAT_VERSION = 1
private const val MAX_VAULT_PAYLOAD_CHARS = 64 * 1024

private val vaultJson = Json {
    encodeDefaults = true
    explicitNulls = false
    ignoreUnknownKeys = false
    classDiscriminator = "source"
}

internal fun LocalSourceSecret.encodeForVault(): String {
    val encoded = vaultJson.encodeToString<StoredLocalSourceSecret>(toStoredSecret())
    require(encoded.length <= MAX_VAULT_PAYLOAD_CHARS) { "Local source credential payload is too large" }
    return encoded
}

internal fun decodeLocalSourceSecret(encoded: String): LocalSourceSecret {
    require(encoded.length <= MAX_VAULT_PAYLOAD_CHARS) { "Local source credential payload is too large" }
    return try {
        vaultJson.decodeFromString<StoredLocalSourceSecret>(encoded).restore()
    } catch (_: Exception) {
        // Serialization exceptions may contain excerpts of malformed input. Never retain
        // them as a cause because the input is itself the credential payload.
        throw IllegalStateException("Local source credential payload is invalid")
    }
}

private fun LocalSourceSecret.toStoredSecret(): StoredLocalSourceSecret = when (this) {
    is XtreamSourceSecret -> StoredLocalSourceSecret.Xtream(
        baseUrl = credentials.baseUrl,
        username = credentials.username,
        password = credentials.password,
        preferredFormat = credentials.preferredFormat,
    )
    is StalkerSourceSecret -> StoredLocalSourceSecret.Stalker(
        portalUrl = credentials.portalUrl,
        macAddress = credentials.macAddress,
        timezone = credentials.timezone,
        language = credentials.language,
        userAgent = credentials.userAgent,
        serialNumber = credentials.serialNumber,
        deviceId = credentials.deviceId,
        deviceId2 = credentials.deviceId2,
        signature = credentials.signature,
    )
    is M3uSourceSecret -> StoredLocalSourceSecret.M3u(
        playlistUrl = playlistUrl,
        xmltvUrl = xmltvUrl,
        headers = headers,
    )
    is StremioAddonSourceSecret -> StoredLocalSourceSecret.StremioAddon(
        manifestUrl = manifestUrl,
        headers = headers,
    )
}

@Serializable
private sealed class StoredLocalSourceSecret {
    abstract val version: Int
    abstract fun restore(): LocalSourceSecret

    protected fun requireCurrentVersion() {
        require(version == VAULT_FORMAT_VERSION)
    }

    final override fun toString(): String = "StoredLocalSourceSecret(<redacted>)"

    @Serializable
    @SerialName("xtream")
    class Xtream(
        override val version: Int = VAULT_FORMAT_VERSION,
        private val baseUrl: String,
        private val username: String,
        private val password: String,
        private val preferredFormat: StreamFormat,
    ) : StoredLocalSourceSecret() {
        override fun restore(): LocalSourceSecret {
            requireCurrentVersion()
            return XtreamSourceSecret(XtreamCredentials(baseUrl, username, password, preferredFormat))
        }
    }

    @Serializable
    @SerialName("stalker")
    class Stalker(
        override val version: Int = VAULT_FORMAT_VERSION,
        private val portalUrl: String,
        private val macAddress: String,
        private val timezone: String,
        private val language: String,
        private val userAgent: String,
        private val serialNumber: String? = null,
        private val deviceId: String? = null,
        private val deviceId2: String? = null,
        private val signature: String? = null,
    ) : StoredLocalSourceSecret() {
        override fun restore(): LocalSourceSecret {
            requireCurrentVersion()
            return StalkerSourceSecret(
                StalkerCredentials(
                    portalUrl = portalUrl,
                    macAddress = macAddress,
                    timezone = timezone,
                    language = language,
                    userAgent = userAgent,
                    serialNumber = serialNumber,
                    deviceId = deviceId,
                    deviceId2 = deviceId2,
                    signature = signature,
                ),
            )
        }
    }

    @Serializable
    @SerialName("m3u")
    class M3u(
        override val version: Int = VAULT_FORMAT_VERSION,
        private val playlistUrl: String,
        private val xmltvUrl: String? = null,
        private val headers: Map<String, String> = emptyMap(),
    ) : StoredLocalSourceSecret() {
        override fun restore(): LocalSourceSecret {
            requireCurrentVersion()
            return M3uSourceSecret(playlistUrl, xmltvUrl, headers)
        }
    }

    @Serializable
    @SerialName("stremio-addon")
    class StremioAddon(
        override val version: Int = VAULT_FORMAT_VERSION,
        private val manifestUrl: String,
        private val headers: Map<String, String> = emptyMap(),
    ) : StoredLocalSourceSecret() {
        override fun restore(): LocalSourceSecret {
            requireCurrentVersion()
            return StremioAddonSourceSecret(manifestUrl, headers)
        }
    }
}
