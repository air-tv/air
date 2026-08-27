package com.getair.core.media

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class MediaRepositoryTest {
    @Test
    fun defaultsToLocalOnlyAndAcceptsAFutureSyncSource() = runTest {
        val store = InMemoryMediaStore(MediaSnapshot(revision = 1))
        val local = LocalFirstMediaRepository(store)
        assertEquals(MediaRefreshResult.LocalOnly, local.refresh())
        assertEquals(1, local.snapshot.value.revision)

        val synced = LocalFirstMediaRepository(store) { previous ->
            previous.copy(revision = previous.revision + 1)
        }
        assertIs<MediaRefreshResult.Updated>(synced.refresh())
        assertEquals(2, synced.snapshot.value.revision)
    }
}
