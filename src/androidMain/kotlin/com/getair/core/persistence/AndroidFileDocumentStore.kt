package com.getair.core.persistence

import android.content.Context
import android.system.Os
import java.io.File
import java.io.FileOutputStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Atomic local-only files under Android's no-backup application directory. */
class AndroidFileDocumentStore(
    context: Context,
    directoryName: String = DEFAULT_DIRECTORY,
    private val maximumDocumentBytes: Long = DEFAULT_MAXIMUM_DOCUMENT_BYTES,
) : LocalDocumentStore {
    private val mutex = Mutex()
    private val directory: File

    init {
        validateLocalDocumentName(directoryName)
        require(maximumDocumentBytes > 0)
        directory = File(context.applicationContext.noBackupFilesDir, directoryName)
    }

    override suspend fun read(document: String): String? = mutex.withLock {
        validateLocalDocumentName(document)
        withContext(Dispatchers.IO) {
            val target = target(document)
            if (!target.exists()) return@withContext null
            require(target.isFile) { "Local application document is not a regular file" }
            require(target.length() <= maximumDocumentBytes) { "Local application document is too large" }
            target.readText(Charsets.UTF_8)
        }
    }

    override suspend fun write(document: String, value: String) {
        validateLocalDocumentName(document)
        val bytes = value.encodeToByteArray()
        require(bytes.size <= maximumDocumentBytes) { "Local application document is too large" }
        mutex.withLock {
            withContext(Dispatchers.IO) {
                check(directory.exists() || directory.mkdirs()) { "Android local state directory could not be created" }
                val temporary = File.createTempFile(".$document.", ".tmp", directory)
                try {
                    FileOutputStream(temporary).use { output ->
                        output.write(bytes)
                        output.fd.sync()
                    }
                    temporary.setReadable(false, false)
                    temporary.setWritable(false, false)
                    check(temporary.setReadable(true, true) && temporary.setWritable(true, true)) {
                        "Android local state permissions could not be restricted"
                    }
                    Os.rename(temporary.absolutePath, target(document).absolutePath)
                } finally {
                    temporary.delete()
                }
            }
        }
    }

    override suspend fun remove(document: String) {
        validateLocalDocumentName(document)
        mutex.withLock {
            withContext(Dispatchers.IO) {
                val target = target(document)
                check(!target.exists() || target.delete()) { "Android local application document could not be removed" }
            }
        }
    }

    private fun target(document: String): File = File(directory, document).also {
        require(it.parentFile == directory) { "Local document path escapes its directory" }
    }

    override fun toString(): String = "AndroidFileDocumentStore(directory=<redacted>)"

    private companion object {
        const val DEFAULT_DIRECTORY = "air-state"
        const val DEFAULT_MAXIMUM_DOCUMENT_BYTES = 16L * 1024 * 1024
    }
}
