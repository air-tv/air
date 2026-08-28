@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package com.getair.core.persistence

import kotlinx.coroutines.test.runTest
import platform.Foundation.NSFileManager
import platform.Foundation.NSTemporaryDirectory
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class AppleFileDocumentStoreTest {
    @Test
    fun atomicallyPersistsAndRemovesAnAppleDocument() = runTest {
        val directory = NSTemporaryDirectory() + "/air-document-test-${Random.nextLong()}"
        try {
            val store = AppleFileDocumentStore(directory)

            assertNull(store.read("household.v1"))
            store.write("household.v1", "first")
            store.write("household.v1", "second")

            assertEquals("second", AppleFileDocumentStore(directory).read("household.v1"))
            assertFalse(directory in store.toString())
            store.remove("household.v1")
            assertNull(store.read("household.v1"))
        } finally {
            NSFileManager.defaultManager.removeItemAtPath(directory, error = null)
        }
    }
}
