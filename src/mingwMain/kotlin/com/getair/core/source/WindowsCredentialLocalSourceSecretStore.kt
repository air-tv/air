@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package com.getair.core.source

import com.getair.core.wincred.CREDENTIALW
import com.getair.core.wincred.CRED_MAX_CREDENTIAL_BLOB_SIZE
import com.getair.core.wincred.CRED_PERSIST_LOCAL_MACHINE
import com.getair.core.wincred.CRED_TYPE_GENERIC
import com.getair.core.wincred.CredDeleteW
import com.getair.core.wincred.CredFree
import com.getair.core.wincred.CredReadW
import com.getair.core.wincred.CredWriteW
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.cinterop.wcstr
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import platform.windows.ERROR_NOT_FOUND
import platform.windows.GetLastError

/**
 * Windows Credential Manager vault. Payloads are generation-chunked because a
 * generic credential blob is limited to [CRED_MAX_CREDENTIAL_BLOB_SIZE] bytes.
 */
class WindowsCredentialLocalSourceSecretStore(
    private val targetPrefix: String = DEFAULT_TARGET_PREFIX,
) : LocalSourceSecretStore {
    private val mutex = Mutex()

    init {
        require(targetPrefix.isNotBlank() && targetPrefix.length <= 128 && '\u0000' !in targetPrefix)
    }

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock {
        val manifest = readCredential(id.manifestTarget()) ?: return@withLock null
        val (generation, count) = decodeManifest(manifest)
        val bytes = List(count) { index ->
            readCredential(id.chunkTarget(generation, index))
                ?: throw IllegalStateException("Windows source credential is incomplete")
        }.flattenBytes()
        decodeLocalSourceSecret(bytes.decodeToString())
    }

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) = mutex.withLock {
        val previous = readCredential(id.manifestTarget())?.let(::decodeManifest)
        val generation = (previous?.first ?: 0L).let { if (it == Long.MAX_VALUE) 1L else it + 1L }
        val chunks = secret.encodeForVault().encodeToByteArray()
            .asList()
            .chunked(CRED_MAX_CREDENTIAL_BLOB_SIZE)
            .map { it.toByteArray() }
        require(chunks.isNotEmpty() && chunks.size <= MAX_CHUNKS) {
            "Windows source credential payload is too large"
        }
        chunks.forEachIndexed { index, chunk -> writeCredential(id.chunkTarget(generation, index), chunk) }
        writeCredential(id.manifestTarget(), encodeManifest(generation, chunks.size))
        previous?.let { (oldGeneration, oldCount) ->
            repeat(oldCount) { index -> removeCredential(id.chunkTarget(oldGeneration, index)) }
        }
        Unit
    }

    override suspend fun remove(id: LocalSourceId) = mutex.withLock {
        readCredential(id.manifestTarget())?.let(::decodeManifest)?.let { (generation, count) ->
            repeat(count) { index -> removeCredential(id.chunkTarget(generation, index)) }
        }
        removeCredential(id.manifestTarget())
    }

    private fun readCredential(target: String): ByteArray? = memScoped {
        val credentialPointer = alloc<CPointerVar<CREDENTIALW>>()
        credentialPointer.value = null
        if (CredReadW(target, CRED_TYPE_GENERIC.toUInt(), 0u, credentialPointer.ptr) == 0) {
            if (GetLastError().toInt() == ERROR_NOT_FOUND) return@memScoped null
            throw IllegalStateException("Windows source credential could not be read")
        }
        val credential = credentialPointer.value
            ?: throw IllegalStateException("Windows Credential Manager returned no source credential")
        try {
            val size = credential.pointed.CredentialBlobSize.toInt()
            val blob = credential.pointed.CredentialBlob?.reinterpret<UByteVar>()
                ?: if (size == 0) return@memScoped byteArrayOf()
                else throw IllegalStateException("Windows source credential data is invalid")
            ByteArray(size) { index -> blob[index].toByte() }
        } finally {
            CredFree(credential)
        }
    }

    private fun writeCredential(targetName: String, bytes: ByteArray) {
        require(bytes.isNotEmpty() && bytes.size <= CRED_MAX_CREDENTIAL_BLOB_SIZE)
        memScoped {
            val target = targetName.wcstr
            val username = USERNAME.wcstr
            bytes.usePinned { pinned ->
                val credential = alloc<CREDENTIALW>().apply {
                    Flags = 0u
                    Type = CRED_TYPE_GENERIC.toUInt()
                    TargetName = target.ptr
                    Comment = null
                    CredentialBlobSize = bytes.size.toUInt()
                    CredentialBlob = pinned.addressOf(0).reinterpret<UByteVar>()
                    Persist = CRED_PERSIST_LOCAL_MACHINE.toUInt()
                    AttributeCount = 0u
                    Attributes = null
                    TargetAlias = null
                    UserName = username.ptr
                }
                if (CredWriteW(credential.ptr, 0u) == 0) {
                    throw IllegalStateException("Windows source credential could not be written")
                }
            }
        }
    }

    private fun removeCredential(target: String) {
        if (CredDeleteW(target, CRED_TYPE_GENERIC.toUInt(), 0u) == 0 &&
            GetLastError().toInt() != ERROR_NOT_FOUND
        ) {
            throw IllegalStateException("Windows source credential could not be removed")
        }
    }

    private fun LocalSourceId.manifestTarget(): String = "$targetPrefix:$value:manifest"
    private fun LocalSourceId.chunkTarget(generation: Long, index: Int): String =
        "$targetPrefix:$value:$generation:$index"

    override fun toString(): String = "WindowsCredentialLocalSourceSecretStore(target=<redacted>)"

    private companion object {
        const val DEFAULT_TARGET_PREFIX = "com.getair.sources.credentials.v1"
        const val USERNAME = "Air TV"
        const val MAX_CHUNKS = 256
    }
}

private fun encodeManifest(generation: Long, count: Int): ByteArray = "$generation:$count".encodeToByteArray()

private fun decodeManifest(value: ByteArray): Pair<Long, Int> {
    val parts = value.decodeToString().split(':')
    val generation = parts.getOrNull(0)?.toLongOrNull()
    val count = parts.getOrNull(1)?.toIntOrNull()
    if (parts.size != 2 || generation == null || generation <= 0 || count == null || count !in 1..256) {
        throw IllegalStateException("Windows source credential manifest is invalid")
    }
    return generation to count
}

private fun List<ByteArray>.flattenBytes(): ByteArray {
    val output = ByteArray(sumOf(ByteArray::size))
    var offset = 0
    forEach { chunk ->
        chunk.copyInto(output, destinationOffset = offset)
        offset += chunk.size
    }
    return output
}
