package com.getair.core.catalog

import com.getair.core.source.LocalSourceId
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class NativeDurableCatalogStoreTest {
    @Test
    fun nativeDriverActivatesAndReadsOneGeneration() = runTest {
        // SQLiter's Windows backend does not consistently honor SQLite's
        // magic in-memory filename across externally supplied SQLite builds.
        // Hosted runners are ephemeral, so use an ordinary private test file.
        val store = openNativeDurableCatalogStore("air-native-catalog-test.db")
        try {
            val source = LocalSourceId("native-source")
            val catalog = DurableCatalogKey(DurableCatalogKind.Stremio, "native")
            val generation = store.beginRefresh(source)
            val meta = MetaPreview("native-item", "movie", "Native Item")
            store.stageCatalogBatch(
                generation,
                catalog,
                listOf(DurableCatalogItem.Stremio(meta)),
            )
            store.activate(generation, CatalogGenerationCounts(1, 0, 0))
            assertEquals(
                meta,
                (store.catalogPage(source, catalog, limit = 1).items.single().item as
                    DurableCatalogItem.Stremio).value,
            )
        } finally {
            store.close()
        }
    }
}
