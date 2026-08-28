package com.getair.core.source

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KWalletLocalSourceSecretStoreTest {
    @Test
    fun readsOnceThenUsesTheMemoryCache() = runTest {
        val encoded = M3uSourceSecret("https://private.invalid/list").encodeForVault()
        val runner = RecordingKWalletSourceRunner(KWalletSourceCommandResult(0, "$encoded\n"))
        val store = KWalletLocalSourceSecretStore(runner)
        val id = LocalSourceId("living-room")

        assertIs<M3uSourceSecret>(store.read(id))
        assertIs<M3uSourceSecret>(store.read(id))

        assertEquals(1, runner.requests.size)
        assertFalse(runner.requests.single().arguments.any { "private.invalid" in it })
    }

    @Test
    fun writesTheCredentialOnlyThroughStandardInput() = runTest {
        val runner = RecordingKWalletSourceRunner(KWalletSourceCommandResult(0))
        val store = KWalletLocalSourceSecretStore(runner)
        val secret = StremioAddonSourceSecret("https://addon.invalid/manifest.json")

        store.write(LocalSourceId("addon"), secret)

        val request = runner.requests.single()
        assertTrue("addon.invalid" in requireNotNull(request.standardInput))
        assertFalse(request.arguments.any { "addon.invalid" in it })
        assertEquals(secret, store.read(LocalSourceId("addon")))
        assertEquals(1, runner.requests.size)
    }

    @Test
    fun removalAndFailuresStayRedacted() = runTest {
        val runner = RecordingKWalletSourceRunner(KWalletSourceCommandResult(0))
        val store = KWalletLocalSourceSecretStore(runner)
        store.remove(LocalSourceId("private-source-id"))
        assertEquals("\n", runner.requests.single().standardInput)
        assertNull(store.read(LocalSourceId("private-source-id")))

        val broken = KWalletLocalSourceSecretStore(
            RecordingKWalletSourceRunner(KWalletSourceCommandResult(2)),
        )
        val error = runCatching { broken.read(LocalSourceId("do-not-print-me")) }.exceptionOrNull()
        assertTrue(error is IllegalStateException)
        assertFalse(error.toString().contains("do-not-print-me"))
    }
}

private class RecordingKWalletSourceRunner(
    private vararg val results: KWalletSourceCommandResult,
) : KWalletSourceCommandRunner {
    val requests = mutableListOf<KWalletSourceRequest>()

    override suspend fun run(
        arguments: List<String>,
        standardInput: String?,
    ): KWalletSourceCommandResult {
        requests += KWalletSourceRequest(arguments, standardInput)
        return results.getOrElse(requests.lastIndex) { results.last() }
    }
}

private data class KWalletSourceRequest(
    val arguments: List<String>,
    val standardInput: String?,
)
