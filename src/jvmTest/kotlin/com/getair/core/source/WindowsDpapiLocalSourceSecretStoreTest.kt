package com.getair.core.source

import java.nio.file.Files
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WindowsDpapiLocalSourceSecretStoreTest {
    @Test
    fun successfulReadCannotSilentlyReturnAnEmptyPayload() = runTest {
        val runner = RecordingPowerShellRunner()
        val directory = createTempDirectory("air-dpapi-empty-test-")
        try {
            val store = WindowsDpapiLocalSourceSecretStore(directory, runner)

            assertFailsWith<IllegalStateException> { store.read(LocalSourceId("missing-output")) }
            assertTrue("Write-Output" in runner.requests.single().script)
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

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
            assertTrue("OpenStandardInput" in request.script)
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
            assertFalse("addon.invalid" in ciphertext, "DPAPI ciphertext retained the synthetic URL")
            assertFalse("synthetic-secret" in ciphertext, "DPAPI ciphertext retained the synthetic header")
            val reopened = WindowsDpapiLocalSourceSecretStore(directory)
            val restored = assertIs<StremioAddonSourceSecret>(
                reopened.read(id),
                "DPAPI read did not restore the expected credential kind",
            )
            assertEquals(secret.manifestUrl, restored.manifestUrl, "DPAPI URL did not round-trip")
            assertEquals(secret.headers, restored.headers, "DPAPI headers did not round-trip")
            reopened.remove(id)
            assertNull(reopened.read(id), "DPAPI removal left a readable credential")
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
