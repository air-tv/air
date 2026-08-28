package com.getair.core.persistence

import java.nio.file.Files
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class JvmFileDocumentStoreTest {
    @Test
    fun atomicallyPersistsReplacesAndRemovesDocuments() = runTest {
        val directory = createTempDirectory("air-state-test-")
        try {
            val store = JvmFileDocumentStore(directory)

            assertNull(store.read("household.v1"))
            store.write("household.v1", "first")
            assertEquals("first", store.read("household.v1"))
            store.write("household.v1", "second")
            assertEquals("second", JvmFileDocumentStore(directory).read("household.v1"))
            assertFalse(Files.list(directory).use { files -> files.anyMatch { it.fileName.toString().endsWith(".tmp") } })
            assertFalse(directory.toString() in store.toString())

            store.remove("household.v1")
            assertNull(store.read("household.v1"))
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun rejectsTraversalNames() = runTest {
        val directory = createTempDirectory("air-state-test-")
        try {
            val store = JvmFileDocumentStore(directory)
            val failure = runCatching { store.write("../outside", "bad") }.exceptionOrNull()
            assertTrue(failure is IllegalArgumentException)
            assertFalse(Files.exists(directory.parent.resolve("outside")))
        } finally {
            directory.toFile().deleteRecursively()
        }
    }
}
