package com.getair.core.source

import java.nio.file.Files
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WindowsDpapiLocalSourceSecretStoreTest {
    @Test
    fun credentialPayloadUsesStdinAndNeverArgvOrEnvironment() = runTest {
        val runner = RecordingPowerShellRunner()
        val directory = createTempDirectory("air-dpapi-test-")
        try {
            val store = WindowsDpapiLocalSourceSecretStore(directory, runner)
            val secret = M3uSourceSecret("https://provider.invalid/list.m3u")

            store.write(LocalSourceId("living-room"), secret)

            val request = runner.requests.single()
            assertTrue(request.standardInput?.isNotBlank() == true)
            assertTrue("Add-Type -AssemblyName System.Security" in request.script)
            assertFalse("provider.invalid" in request.script)
            assertFalse("provider.invalid" in request.target.toString())
            assertEquals(secret, store.read(LocalSourceId("living-room")))
            assertEquals(1, runner.requests.size)
            assertFalse(directory.toString() in store.toString())
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun realWindowsDpapiRoundTripUsesEncryptedBytes() = runTest {
        if (!System.getProperty("os.name").contains("Windows", ignoreCase = true)) return@runTest
        val directory = createTempDirectory("air-dpapi-integration-")
        try {
            val store = WindowsDpapiLocalSourceSecretStore(directory)
            val id = LocalSourceId("windows-integration")
            val secret = StremioAddonSourceSecret(
                "https://addon.invalid/manifest.json",
                mapOf("Authorization" to "Bearer synthetic-secret"),
            )

            store.write(id, secret)
            val persisted = Files.list(directory).use { files -> files.findFirst().orElseThrow() }
            val ciphertext = Files.readAllBytes(persisted).toString(Charsets.ISO_8859_1)
            assertFalse("addon.invalid" in ciphertext)
            assertFalse("synthetic-secret" in ciphertext)
            val reopened = WindowsDpapiLocalSourceSecretStore(directory)
            val restored = assertIs<StremioAddonSourceSecret>(reopened.read(id))
            assertEquals(secret.manifestUrl, restored.manifestUrl)
            assertEquals(secret.headers, restored.headers)
            reopened.remove(id)
            assertNull(reopened.read(id))
        } finally {
            directory.toFile().deleteRecursively()
        }
    }
}

private class RecordingPowerShellRunner : WindowsPowerShellRunner {
    val requests = mutableListOf<PowerShellRequest>()

    override suspend fun run(
        script: String,
        target: java.nio.file.Path,
        standardInput: String?,
    ): WindowsPowerShellResult {
        requests += PowerShellRequest(script, target, standardInput)
        return WindowsPowerShellResult(0)
    }
}

private data class PowerShellRequest(
    val script: String,
    val target: java.nio.file.Path,
    val standardInput: String?,
)
