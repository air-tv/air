package com.getair.core.source

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

/** KDE Wallet adapter with no libsecret dependency and no credentials in argv or env. */
class KWalletLocalSourceSecretStore private constructor(
    private val wallet: String,
    private val folder: String,
    private val runner: KWalletSourceCommandRunner,
) : LocalSourceSecretStore {
    constructor(
        wallet: String = DEFAULT_WALLET,
        folder: String = DEFAULT_FOLDER,
        executable: String = DEFAULT_EXECUTABLE,
        commandTimeoutMillis: Long = DEFAULT_TIMEOUT_MILLIS,
    ) : this(wallet, folder, ProcessKWalletSourceCommandRunner(executable, commandTimeoutMillis))

    internal constructor(
        commandRunner: KWalletSourceCommandRunner,
        walletName: String = DEFAULT_WALLET,
        folderName: String = DEFAULT_FOLDER,
    ) : this(walletName, folderName, commandRunner)

    private val mutex = Mutex()
    private val cache = mutableMapOf<LocalSourceId, CachedSourceSecret>()

    init {
        require(wallet.isSafeKWalletArgument())
        require(folder.isSafeKWalletArgument())
    }

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock {
        cache[id]?.let { return@withLock it.value }
        val result = runner.run(
            arguments = listOf("--read-password", id.kwalletEntry(), "--folder", folder, wallet),
            standardInput = null,
        )
        val value = when (result.exitCode) {
            KWALLET_SUCCESS -> result.standardOutput.trimLineEnding().ifEmpty { null }
                ?.let(::decodeLocalSourceSecret)
            KWALLET_OPERATION_FAILED -> null
            else -> throw IllegalStateException("KDE Wallet source credential could not be read")
        }
        cache[id] = CachedSourceSecret(value)
        value
    }

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        val encoded = secret.encodeForVault()
        mutex.withLock {
            writeEntry(id, encoded)
            cache[id] = CachedSourceSecret(secret)
        }
    }

    override suspend fun remove(id: LocalSourceId) {
        mutex.withLock {
            // kwallet-query cannot delete. An empty password clears the secret value.
            writeEntry(id, "")
            cache[id] = CachedSourceSecret(null)
        }
    }

    private suspend fun writeEntry(id: LocalSourceId, value: String) {
        val result = runner.run(
            arguments = listOf("--write-password", id.kwalletEntry(), "--folder", folder, wallet),
            standardInput = "$value\n",
        )
        if (result.exitCode != KWALLET_SUCCESS) {
            throw IllegalStateException("KDE Wallet source credential could not be written")
        }
    }

    override fun toString(): String =
        "KWalletLocalSourceSecretStore(wallet=<redacted>, folder=<redacted>, cache=<redacted>)"

    private companion object {
        const val DEFAULT_WALLET = "kdewallet"
        const val DEFAULT_FOLDER = "Air TV"
        const val DEFAULT_EXECUTABLE = "kwallet-query"
        const val DEFAULT_TIMEOUT_MILLIS = 15_000L
        const val KWALLET_SUCCESS = 0
        const val KWALLET_OPERATION_FAILED = 4
    }
}

internal fun interface KWalletSourceCommandRunner {
    suspend fun run(arguments: List<String>, standardInput: String?): KWalletSourceCommandResult
}

internal data class KWalletSourceCommandResult(
    val exitCode: Int,
    val standardOutput: String = "",
)

private class ProcessKWalletSourceCommandRunner(
    private val executable: String,
    private val timeoutMillis: Long,
) : KWalletSourceCommandRunner {
    init {
        require(executable.isSafeKWalletArgument())
        require(timeoutMillis > 0)
    }

    override suspend fun run(
        arguments: List<String>,
        standardInput: String?,
    ): KWalletSourceCommandResult = withContext(Dispatchers.IO) {
        val process = ProcessBuilder(listOf(executable) + arguments)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
        try {
            process.outputStream.use { output ->
                if (standardInput != null) output.write(standardInput.toByteArray(StandardCharsets.UTF_8))
            }
            if (!process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly()
                throw IllegalStateException("KDE Wallet source credential command timed out")
            }
            KWalletSourceCommandResult(
                exitCode = process.exitValue(),
                standardOutput = process.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() },
            )
        } finally {
            if (process.isAlive) process.destroyForcibly()
        }
    }
}

private data class CachedSourceSecret(val value: LocalSourceSecret?)

private fun LocalSourceId.kwalletEntry(): String = "source-" + MessageDigest.getInstance("SHA-256")
    .digest(value.encodeToByteArray())
    .joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }

private fun String.isSafeKWalletArgument(): Boolean = isNotBlank() && length <= 255 && '\u0000' !in this
private fun String.trimLineEnding(): String = trimEnd('\n', '\r')
