@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package com.getair.core.persistence

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import platform.Foundation.NSData
import platform.Foundation.NSFileManager
import platform.Foundation.NSString
import platform.Foundation.NSUTF8StringEncoding
import platform.Foundation.create
import platform.Foundation.dataUsingEncoding
import platform.Foundation.dataWithContentsOfFile
import platform.Foundation.defaultManager
import platform.Foundation.length
import platform.Foundation.writeToFile

/** Atomic Foundation files in the app-support directory supplied by an Apple shell. */
class AppleFileDocumentStore(
    directoryPath: String,
    private val maximumDocumentBytes: ULong = DEFAULT_MAXIMUM_DOCUMENT_BYTES,
) : LocalDocumentStore {
    private val mutex = Mutex()
    private val directory = directoryPath.trimEnd('/')
    private val files = NSFileManager.defaultManager

    init {
        require(directory.isNotBlank() && directory.length <= 4_096 && '\u0000' !in directory)
        require(maximumDocumentBytes > 0uL)
    }

    override suspend fun read(document: String): String? = mutex.withLock {
        validateLocalDocumentName(document)
        withContext(Dispatchers.Default) {
            val path = target(document)
            if (!files.fileExistsAtPath(path)) return@withContext null
            val data = NSData.dataWithContentsOfFile(path)
                ?: throw IllegalStateException("Apple local application document could not be read")
            require(data.length <= maximumDocumentBytes) { "Local application document is too large" }
            NSString.create(data, NSUTF8StringEncoding) as String?
                ?: throw IllegalStateException("Apple local application document is not UTF-8")
        }
    }

    override suspend fun write(document: String, value: String) {
        validateLocalDocumentName(document)
        val data = NSString.create(string = value).dataUsingEncoding(NSUTF8StringEncoding)
            ?: throw IllegalStateException("Apple local application document could not be encoded")
        require(data.length <= maximumDocumentBytes) { "Local application document is too large" }
        mutex.withLock {
            withContext(Dispatchers.Default) {
                check(
                    files.fileExistsAtPath(directory) || files.createDirectoryAtPath(
                        directory,
                        withIntermediateDirectories = true,
                        attributes = null,
                        error = null,
                    ),
                ) { "Apple local state directory could not be created" }
                check(data.writeToFile(target(document), atomically = true)) {
                    "Apple local application document could not be written"
                }
            }
        }
    }

    override suspend fun remove(document: String) {
        validateLocalDocumentName(document)
        mutex.withLock {
            withContext(Dispatchers.Default) {
                val path = target(document)
                check(!files.fileExistsAtPath(path) || files.removeItemAtPath(path, error = null)) {
                    "Apple local application document could not be removed"
                }
            }
        }
    }

    private fun target(document: String): String = "$directory/$document"

    override fun toString(): String = "AppleFileDocumentStore(directory=<redacted>)"

    private companion object {
        const val DEFAULT_MAXIMUM_DOCUMENT_BYTES: ULong = 16uL * 1024uL * 1024uL
    }
}
