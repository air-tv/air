package com.getair.core.source

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * Windows JVM vault using current-user DPAPI. Credential payloads travel only
 * through stdin; argv and environment contain no provider data.
 */
class WindowsDpapiLocalSourceSecretStore private constructor(
    directory: Path,
    private val runner: WindowsPowerShellRunner,
    @Suppress("UNUSED_PARAMETER") marker: Unit,
) : LocalSourceSecretStore {
    constructor(
        directory: Path,
        powershellExecutable: String = DEFAULT_POWERSHELL,
        commandTimeoutMillis: Long = DEFAULT_TIMEOUT_MILLIS,
    ) : this(directory, ProcessWindowsPowerShellRunner(powershellExecutable, commandTimeoutMillis), Unit)

    internal constructor(directory: Path, commandRunner: WindowsPowerShellRunner) : this(
        directory,
        runner = commandRunner,
        marker = Unit,
    )

    private val mutex = Mutex()
    private val directory = directory.toAbsolutePath().normalize()
    private val cache = mutableMapOf<LocalSourceId, DpapiCachedSourceSecret>()

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock {
        cache[id]?.let { return@withLock it.value }
        val result = runner.run(READ_SCRIPT, target(id), null)
        val value = when (result.exitCode) {
            POWERSHELL_SUCCESS -> result.standardOutput.trim().ifEmpty { null }?.let { encoded ->
                val document = try {
                    Base64.getDecoder().decode(encoded).toString(StandardCharsets.UTF_8)
                } catch (_: IllegalArgumentException) {
                    throw IllegalStateException("Windows source credential output is invalid")
                }
                decodeLocalSourceSecret(document)
            }
            POWERSHELL_NOT_FOUND -> null
            else -> throw IllegalStateException("Windows source credential could not be read")
        }
        cache[id] = DpapiCachedSourceSecret(value)
        value
    }

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        val payload = Base64.getEncoder().encodeToString(secret.encodeForVault().toByteArray(StandardCharsets.UTF_8))
        mutex.withLock {
            val result = runner.run(WRITE_SCRIPT, target(id), payload)
            if (result.exitCode != POWERSHELL_SUCCESS) {
                throw IllegalStateException("Windows source credential could not be written")
            }
            cache[id] = DpapiCachedSourceSecret(secret)
        }
    }

    override suspend fun remove(id: LocalSourceId) {
        mutex.withLock {
            val result = runner.run(REMOVE_SCRIPT, target(id), null)
            if (result.exitCode != POWERSHELL_SUCCESS) {
                throw IllegalStateException("Windows source credential could not be removed")
            }
            cache[id] = DpapiCachedSourceSecret(null)
        }
    }

    private fun target(id: LocalSourceId): Path = directory.resolve(id.dpapiEntry() + ".bin").normalize().also {
        require(it.parent == directory) { "Windows source credential path escapes its directory" }
    }

    override fun toString(): String =
        "WindowsDpapiLocalSourceSecretStore(directory=<redacted>, cache=<redacted>)"

    private companion object {
        const val DEFAULT_POWERSHELL = "powershell.exe"
        const val DEFAULT_TIMEOUT_MILLIS = 15_000L
        const val POWERSHELL_SUCCESS = 0
        const val POWERSHELL_NOT_FOUND = 3
    }
}

internal fun interface WindowsPowerShellRunner {
    suspend fun run(script: String, target: Path, standardInput: String?): WindowsPowerShellResult
}

internal data class WindowsPowerShellResult(
    val exitCode: Int,
    val standardOutput: String = "",
)

private class ProcessWindowsPowerShellRunner(
    private val executable: String,
    private val timeoutMillis: Long,
) : WindowsPowerShellRunner {
    init {
        require(executable.isNotBlank() && executable.length <= 255 && '\u0000' !in executable)
        require(timeoutMillis > 0)
    }

    override suspend fun run(
        script: String,
        target: Path,
        standardInput: String?,
    ): WindowsPowerShellResult = withContext(Dispatchers.IO) {
        val encodedScript = Base64.getEncoder().encodeToString(script.toByteArray(StandardCharsets.UTF_16LE))
        val process = ProcessBuilder(
            executable,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encodedScript,
        )
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .apply { environment()[TARGET_ENVIRONMENT] = target.toString() }
            .start()
        try {
            process.outputStream.use { output ->
                if (standardInput != null) output.write(standardInput.toByteArray(StandardCharsets.UTF_8))
            }
            if (!process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly()
                throw IllegalStateException("Windows source credential command timed out")
            }
            val output = process.inputStream.readNBytes(MAXIMUM_OUTPUT_BYTES + 1)
            require(output.size <= MAXIMUM_OUTPUT_BYTES) { "Windows source credential output is too large" }
            WindowsPowerShellResult(process.exitValue(), output.toString(StandardCharsets.UTF_8))
        } finally {
            if (process.isAlive) process.destroyForcibly()
        }
    }

    private companion object {
        const val TARGET_ENVIRONMENT = "AIR_TV_VAULT_TARGET"
        const val MAXIMUM_OUTPUT_BYTES = 128 * 1024
    }
}

private data class DpapiCachedSourceSecret(val value: LocalSourceSecret?)

private fun LocalSourceId.dpapiEntry(): String = MessageDigest.getInstance("SHA-256")
    .digest(value.encodeToByteArray())
    .joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }

private val WRITE_SCRIPT = """
    ${'$'}ErrorActionPreference = 'Stop'
    Add-Type -AssemblyName System.Security
    ${'$'}path = ${'$'}env:AIR_TV_VAULT_TARGET
    ${'$'}input = [Console]::In.ReadToEnd().Trim()
    ${'$'}plain = [Convert]::FromBase64String(${'$'}input)
    ${'$'}protected = [Security.Cryptography.ProtectedData]::Protect(
        ${'$'}plain,
        ${'$'}null,
        [Security.Cryptography.DataProtectionScope]::CurrentUser
    )
    ${'$'}directory = [IO.Path]::GetDirectoryName(${'$'}path)
    [IO.Directory]::CreateDirectory(${'$'}directory) | Out-Null
    ${'$'}temporary = "${'$'}path.${'$'}PID.tmp"
    try {
        [IO.File]::WriteAllBytes(${'$'}temporary, ${'$'}protected)
        Move-Item -LiteralPath ${'$'}temporary -Destination ${'$'}path -Force
    } finally {
        if (Test-Path -LiteralPath ${'$'}temporary) { Remove-Item -LiteralPath ${'$'}temporary -Force }
    }
""".trimIndent()

private val READ_SCRIPT = """
    ${'$'}ErrorActionPreference = 'Stop'
    Add-Type -AssemblyName System.Security
    ${'$'}path = ${'$'}env:AIR_TV_VAULT_TARGET
    if (-not (Test-Path -LiteralPath ${'$'}path -PathType Leaf)) { exit 3 }
    ${'$'}protected = [IO.File]::ReadAllBytes(${'$'}path)
    ${'$'}plain = [Security.Cryptography.ProtectedData]::Unprotect(
        ${'$'}protected,
        ${'$'}null,
        [Security.Cryptography.DataProtectionScope]::CurrentUser
    )
    [Console]::Out.Write([Convert]::ToBase64String(${'$'}plain))
""".trimIndent()

private val REMOVE_SCRIPT = """
    ${'$'}ErrorActionPreference = 'Stop'
    ${'$'}path = ${'$'}env:AIR_TV_VAULT_TARGET
    if (Test-Path -LiteralPath ${'$'}path) { Remove-Item -LiteralPath ${'$'}path -Force }
""".trimIndent()
