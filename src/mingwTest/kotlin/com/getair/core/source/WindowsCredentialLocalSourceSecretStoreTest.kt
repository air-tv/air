package com.getair.core.source

import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class WindowsCredentialLocalSourceSecretStoreTest {
    @Test
    fun roundTripsAndRemovesAChunkedCredential() = runTest {
        val store = WindowsCredentialLocalSourceSecretStore(
            targetPrefix = "com.getair.sources.credentials.test.v1",
        )
        val id = LocalSourceId("windows-ci-${Random.nextLong()}")
        val secret = M3uSourceSecret(
            playlistUrl = "https://provider.invalid/list.m3u",
            xmltvUrl = "https://provider.invalid/guide.xml",
            headers = mapOf("X-Large-Synthetic-Test" to "x".repeat(3_000)),
        )

        try {
            store.write(id, secret)

            val restored = assertIs<M3uSourceSecret>(store.read(id))
            assertEquals(secret.playlistUrl, restored.playlistUrl)
            assertEquals(secret.xmltvUrl, restored.xmltvUrl)
            assertEquals(secret.headers, restored.headers)

            store.remove(id)
            assertNull(store.read(id))
        } finally {
            try {
                store.remove(id)
            } catch (_: Exception) {
                // Preserve the original assertion if best-effort CI cleanup also fails.
            }
        }
    }
}
