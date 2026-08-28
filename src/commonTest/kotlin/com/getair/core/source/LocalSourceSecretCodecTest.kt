package com.getair.core.source

import com.getair.iptv.StalkerCredentials
import com.getair.iptv.XtreamCredentials
import com.getair.iptv.model.StreamFormat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class LocalSourceSecretCodecTest {
    @Test
    fun roundTripsEverySourceKind() {
        val values = listOf<LocalSourceSecret>(
            XtreamSourceSecret(XtreamCredentials("https://x.invalid", "user", "pass", StreamFormat.Ts)),
            StalkerSourceSecret(
                StalkerCredentials(
                    portalUrl = "https://portal.invalid",
                    macAddress = "00:11:22:33:44:55",
                    timezone = "America/Detroit",
                    language = "en",
                    userAgent = "Air",
                    serialNumber = "serial",
                    deviceId = "one",
                    deviceId2 = "two",
                    signature = "signature",
                ),
            ),
            M3uSourceSecret(
                playlistUrl = "https://m3u.invalid/list",
                xmltvUrl = "https://m3u.invalid/guide",
                headers = mapOf("Authorization" to "Bearer secret"),
            ),
            StremioAddonSourceSecret(
                manifestUrl = "https://addon.invalid/manifest.json",
                headers = mapOf("X-Token" to "secret"),
            ),
        )

        val restored = values.map { decodeLocalSourceSecret(it.encodeForVault()) }

        restored.zip(values).forEach { (actual, expected) ->
            assertEquals(expected.kind, actual.kind)
        }
        val xtream = assertIs<XtreamSourceSecret>(restored[0]).credentials
        assertEquals("https://x.invalid", xtream.baseUrl)
        assertEquals("user", xtream.username)
        assertEquals("pass", xtream.password)
        assertEquals(StreamFormat.Ts, xtream.preferredFormat)
        val stalker = assertIs<StalkerSourceSecret>(restored[1]).credentials
        assertEquals("00:11:22:33:44:55", stalker.macAddress)
        assertEquals("signature", stalker.signature)
        assertEquals("Bearer secret", assertIs<M3uSourceSecret>(restored[2]).headers["Authorization"])
        assertEquals("secret", assertIs<StremioAddonSourceSecret>(restored[3]).headers["X-Token"])
    }

    @Test
    fun decodeFailureDoesNotEchoCredentialInput() {
        val secret = "do-not-leak-this-provider-token"

        val error = assertFailsWith<IllegalStateException> {
            decodeLocalSourceSecret("{\"password\":\"$secret\"}")
        }

        assertFalse(secret in error.toString())
        assertEquals(null, error.cause)
    }
}
