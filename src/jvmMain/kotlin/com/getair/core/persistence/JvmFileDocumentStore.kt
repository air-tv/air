package com.getair.core.persistence

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermission
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Atomic, owner-private JVM document storage for desktop application shells. */
class JvmFileDocumentStore(
    directory: Path,
    private val maximumDocumentBytes: Long = DEFAULT_MAXIMUM_DOCUMENT_BYTES,
) : LocalDocumentStore {
    private val mutex = Mutex()
    private val directory = directory.toAbsolutePath().normalize()

    init {
        require(maximumDocumentBytes > 0)
    }

    override suspend fun read(document: String): String? = mutex.withLock {
        validateLocalDocumentName(document)
        withContext(Dispatchers.IO) {
            val target = target(document)
            if (!Files.exists(target)) return@withContext null
            require(Files.isRegularFile(target)) { "Local application document is not a regular file" }
            require(Files.size(target) <= maximumDocumentBytes) { "Local application document is too large" }
            Files.readString(target, StandardCharsets.UTF_8)
        }
    }

    override suspend fun write(document: String, value: String) {
        validateLocalDocumentName(document)
        val bytes = value.encodeToByteArray()
        require(bytes.size <= maximumDocumentBytes) { "Local application document is too large" }
        mutex.withLock {
            withContext(Dispatchers.IO) {
                Files.createDirectories(directory)
                restrictDirectory(directory)
                val temporary = Files.createTempFile(directory, ".$document.", ".tmp")
                try {
                    Files.write(
                        temporary,
                        bytes,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                    )
                    restrictFile(temporary)
                    FileChannel.open(temporary, StandardOpenOption.WRITE).use { it.force(true) }
                    moveAtomically(temporary, target(document))
                } finally {
                    Files.deleteIfExists(temporary)
                }
            }
        }
    }

    override suspend fun remove(document: String) {
        validateLocalDocumentName(document)
        mutex.withLock {
            withContext(Dispatchers.IO) { Files.deleteIfExists(target(document)) }
        }
    }

    private fun target(document: String): Path = directory.resolve(document).normalize().also {
        require(it.parent == directory) { "Local document path escapes its directory" }
    }

    private fun moveAtomically(source: Path, target: Path) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun restrictDirectory(path: Path) {
        try {
            Files.setPosixFilePermissions(
                path,
                setOf(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                ),
            )
        } catch (_: UnsupportedOperationException) {
            // Windows ACLs are inherited from the user's application-data directory.
        }
    }

    private fun restrictFile(path: Path) {
        try {
            Files.setPosixFilePermissions(
                path,
                setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
            )
        } catch (_: UnsupportedOperationException) {
            // Windows ACLs are inherited from the user's application-data directory.
        }
    }

    override fun toString(): String = "JvmFileDocumentStore(directory=<redacted>)"

    private companion object {
        const val DEFAULT_MAXIMUM_DOCUMENT_BYTES = 16L * 1024 * 1024
    }
}
