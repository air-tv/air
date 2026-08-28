package com.getair.core.catalog

import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.QueryResult
import com.getair.core.catalog.db.AirCatalogDatabase
import com.getair.core.catalog.db.CatalogQueries
import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgNowNext
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.IptvChannelMetadata
import com.getair.iptv.model.IptvEpisodeMetadata
import com.getair.iptv.model.IptvMovieMetadata
import com.getair.iptv.model.IptvSeriesMetadata
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private const val PAYLOAD_VERSION = 1L

internal fun interface CatalogWriteProbe {
    fun beforeItem(index: Int)

    companion object {
        val None = CatalogWriteProbe { }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
internal class SqlDelightDurableCatalogStore(
    private val driver: SqlDriver,
    dispatcher: CoroutineDispatcher = Dispatchers.Default.limitedParallelism(1),
    private val nowMillis: () -> Long = { Clock.System.now().toEpochMilliseconds() },
    private val writeProbe: CatalogWriteProbe = CatalogWriteProbe.None,
    guideReadProbe: GuideReadProbe = GuideReadProbe.None,
) : DurableCatalogStore {
    private val database = AirCatalogDatabase(driver)
    private val queries: CatalogQueries = database.catalogQueries
    private val databaseDispatcher = dispatcher
    private val writer = Mutex()

    override val guides: DurableGuideStore = SqlDelightDurableGuideStore(
        driver = driver,
        database = database,
        dispatcher = databaseDispatcher,
        writer = writer,
        nowMillis = nowMillis,
        readProbe = guideReadProbe,
    )

    override suspend fun beginRefresh(sourceId: LocalSourceId): CatalogGeneration = serializedWrite {
        var generation = 0L
        database.transaction {
            queries.ensureSource(sourceId.value)
            queries.prepareSourceRefresh(sourceId.value)
            val state = queries.sourceState(sourceId.value).executeAsOne()
            generation = state.next_generation
            check(generation > 0 && generation < Long.MAX_VALUE) {
                "Catalog generation space is exhausted"
            }
            queries.advanceGeneration(sourceId.value, generation)
            queries.insertGeneration(sourceId.value, generation, nowMillis())
        }
        CatalogGeneration(sourceId, generation)
    }

    override suspend fun stageCatalogBatch(
        generation: CatalogGeneration,
        catalog: DurableCatalogKey,
        items: List<DurableCatalogItem>,
    ) {
        requireBatch(items.size)
        items.forEach { item -> requireItemMatches(catalog.kind, item) }
        val encoded = items.map(::encodeCatalogItem)
        probedWrite { context ->
            requireStaging(generation)
            database.transaction {
                var order = queries.nextCatalogOrder(
                    generation.sourceId.value,
                    generation.value,
                    catalog.kind.storageValue,
                    catalog.catalogId,
                ).executeAsOne()
                encoded.forEachIndexed { index, item ->
                    context.ensureActive()
                    writeProbe.beforeItem(index)
                    context.ensureActive()
                    queries.insertCatalogEntry(
                        source_id = generation.sourceId.value,
                        generation = generation.value,
                        kind = catalog.kind.storageValue,
                        catalog_id = catalog.catalogId,
                        entity_id = item.entityId,
                        sort_order = order++,
                        display_name = item.displayName,
                        payload_version = PAYLOAD_VERSION,
                        payload = item.payload,
                    )
                }
            }
        }
    }

    override suspend fun stageGuideBatch(
        generation: CatalogGeneration,
        channels: List<EpgChannel>,
        programmes: List<DurableGuideProgramme>,
    ) {
        require(channels.size <= MAX_BATCH_ITEMS && programmes.size <= MAX_BATCH_ITEMS) {
            "Guide batches are limited to $MAX_BATCH_ITEMS channels and programmes"
        }
        require(channels.isNotEmpty() || programmes.isNotEmpty()) { "A guide batch cannot be empty" }
        val encodedChannels = channels.map(::encodeChannel)
        val encodedProgrammes = programmes.map(::encodeProgramme)
        probedWrite { context ->
            requireStaging(generation)
            database.transaction {
                var order = queries.nextChannelOrder(
                    generation.sourceId.value,
                    generation.value,
                ).executeAsOne()
                encodedChannels.forEachIndexed { index, channel ->
                    context.ensureActive()
                    writeProbe.beforeItem(index)
                    context.ensureActive()
                    queries.insertEpgChannel(
                        source_id = generation.sourceId.value,
                        generation = generation.value,
                        channel_id = channel.id,
                        sort_order = order++,
                        display_name = channel.displayName,
                        payload_version = PAYLOAD_VERSION,
                        payload = channel.payload,
                    )
                }
                encodedProgrammes.forEachIndexed { index, programme ->
                    context.ensureActive()
                    writeProbe.beforeItem(encodedChannels.size + index)
                    context.ensureActive()
                    queries.insertEpgProgramme(
                        source_id = generation.sourceId.value,
                        generation = generation.value,
                        channel_id = programme.channelId,
                        start_ms = programme.startMs,
                        end_ms = programme.endMs,
                        event_id = programme.eventId,
                        payload_version = PAYLOAD_VERSION,
                        payload = programme.payload,
                    )
                }
            }
        }
    }

    override suspend fun activate(
        generation: CatalogGeneration,
        expected: CatalogGenerationCounts,
    ): CatalogSourceStatus = serializedWrite {
        var status: CatalogSourceStatus? = null
        database.transaction {
            requireStaging(generation)
            val actual = generationCounts(generation)
            if (actual != expected) throw CatalogStoreException("Staged catalog counts do not match")
            val activatedAt = nowMillis()
            queries.activateGeneration(generation.value, activatedAt, generation.sourceId.value)
            status = queries.sourceState(generation.sourceId.value).executeAsOne().toStatus()
        }
        checkNotNull(status)
    }

    override suspend fun sourceStatus(sourceId: LocalSourceId): CatalogSourceStatus? = read {
        queries.sourceState(sourceId.value).executeAsOneOrNull()
            ?.takeUnless { it.deleted != 0L }
            ?.toStatus()
    }

    override suspend fun catalogPage(
        sourceId: LocalSourceId,
        catalog: DurableCatalogKey,
        afterSortOrder: Long?,
        limit: Int,
    ): DurableCatalogPage = read {
        require(limit in 1..MAX_CATALOG_PAGE_ITEMS) {
            "Catalog page limit must be between 1 and $MAX_CATALOG_PAGE_ITEMS"
        }
        val rows = queries.catalogPage(
            source_id = sourceId.value,
            kind = catalog.kind.storageValue,
            catalog_id = catalog.catalogId,
            sort_order = afterSortOrder ?: -1L,
            value_ = limit.toLong(),
        ).executeAsList()
        val items = rows.map { row ->
            requirePayloadVersion(row.payload_version)
            DurableCatalogPageItem(row.sort_order, decodeCatalogItem(catalog.kind, row.payload))
        }
        DurableCatalogPage(items, items.lastOrNull()?.sortOrder?.takeIf { items.size == limit })
    }

    override suspend fun channelPage(
        sourceId: LocalSourceId,
        afterSortOrder: Long?,
        limit: Int,
    ): DurableChannelPage = read {
        require(limit in 1..MAX_CHANNEL_PAGE_ITEMS) {
            "Channel page limit must be between 1 and $MAX_CHANNEL_PAGE_ITEMS"
        }
        val rows = queries.channelPage(
            source_id = sourceId.value,
            sort_order = afterSortOrder ?: -1L,
            value_ = limit.toLong(),
        ).executeAsList()
        val items = rows.map { row ->
            requirePayloadVersion(row.payload_version)
            DurableChannelPageItem(row.sort_order, decode(EpgChannel.serializer(), row.payload))
        }
        DurableChannelPage(items, items.lastOrNull()?.sortOrder?.takeIf { items.size == limit })
    }

    override suspend fun guideWindow(
        sourceId: LocalSourceId,
        channelId: com.getair.iptv.model.EpgChannelId,
        from: Instant,
        until: Instant,
        limit: Int,
    ): List<DurableGuideProgramme> = read {
        val fromMs = from.toEpochMilliseconds()
        val untilMs = until.toEpochMilliseconds()
        val durationMs = untilMs - fromMs
        require(durationMs in 1..MAX_GUIDE_WINDOW_MILLIS) {
            "Guide window must be positive and no longer than 14 days"
        }
        require(limit in 1..MAX_GUIDE_WINDOW_ITEMS) {
            "Guide window limit must be between 1 and $MAX_GUIDE_WINDOW_ITEMS"
        }
        queries.guideWindow(
            source_id = sourceId.value,
            channel_id = channelId.value,
            start_ms = untilMs,
            end_ms = fromMs,
            value_ = limit.toLong(),
        ).executeAsList().map { row ->
            requirePayloadVersion(row.payload_version)
            DurableGuideProgramme(row.event_id, decode(EpgProgramme.serializer(), row.payload))
        }
    }

    override suspend fun nowNext(
        sourceId: LocalSourceId,
        channelId: com.getair.iptv.model.EpgChannelId,
        at: Instant,
    ): EpgNowNext = read {
        val atMs = at.toEpochMilliseconds()
        val current = queries.currentProgramme(
            source_id = sourceId.value,
            channel_id = channelId.value,
            start_ms = atMs,
            end_ms = atMs,
        ).executeAsOneOrNull()?.let { row ->
            requirePayloadVersion(row.payload_version)
            decode(EpgProgramme.serializer(), row.payload)
        }
        val next = queries.nextProgramme(
            source_id = sourceId.value,
            channel_id = channelId.value,
            start_ms = atMs,
        ).executeAsOneOrNull()?.let { row ->
            requirePayloadVersion(row.payload_version)
            decode(EpgProgramme.serializer(), row.payload)
        }
        EpgNowNext(current, next)
    }

    override suspend fun deleteSource(sourceId: LocalSourceId) {
        serializedWrite { queries.markSourceDeleted(sourceId.value) }
    }

    override suspend fun cleanupUnreachable(maxRows: Int): CatalogCleanupResult = serializedWrite {
        require(maxRows in 1..10_000) { "Cleanup row limit must be between 1 and 10000" }
        var remaining = maxRows.toLong()
        var removed = 0L
        val orphan = queries.orphanGenerations(1).executeAsOneOrNull()
        if (orphan != null) {
            database.transaction {
                val before = generationRowCount(orphan.source_id, orphan.generation)
                queries.deleteCatalogChunk(orphan.source_id, orphan.generation, remaining)
                val afterCatalog = generationRowCount(orphan.source_id, orphan.generation)
                remaining -= (before - afterCatalog).coerceAtLeast(0)
                if (remaining > 0) {
                    queries.deleteChannelChunk(orphan.source_id, orphan.generation, remaining)
                    val afterChannel = generationRowCount(orphan.source_id, orphan.generation)
                    remaining -= (afterCatalog - afterChannel).coerceAtLeast(0)
                    if (remaining > 0) {
                        queries.deleteProgrammeChunk(orphan.source_id, orphan.generation, remaining)
                    }
                }
                val after = generationRowCount(orphan.source_id, orphan.generation)
                removed = before - after
                deleteGenerationIfEmpty(orphan.source_id, orphan.generation)
                queries.deleteEmptySource(orphan.source_id, orphan.source_id)
            }
        }
        CatalogCleanupResult(
            removedRows = removed.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(),
            hasMore = queries.orphanGenerations(1).executeAsOneOrNull() != null,
        )
    }

    override fun close() {
        driver.close()
    }

    internal suspend fun queryPlansForTest(): Pair<List<String>, List<String>> = read {
        explain(
            """
            EXPLAIN QUERY PLAN
            SELECT c.payload
            FROM catalog_entry AS c
            JOIN catalog_source AS s
              ON s.source_id = c.source_id AND s.active_generation = c.generation
            WHERE c.source_id = ? AND s.deleted = 0
              AND c.kind = ? AND c.catalog_id = ? AND c.sort_order > ?
            ORDER BY c.sort_order, c.entity_id
            LIMIT ?
            """.trimIndent(),
            listOf("source", "stremio", "catalog", -1L, 10L),
        ) to explain(
            """
            EXPLAIN QUERY PLAN
            SELECT p.payload
            FROM epg_programme AS p
            JOIN catalog_source AS s
              ON s.source_id = p.source_id AND s.active_generation = p.generation
            WHERE p.source_id = ? AND s.deleted = 0 AND p.channel_id = ?
              AND p.start_ms < ? AND p.end_ms > ?
            ORDER BY p.start_ms, p.event_id
            LIMIT ?
            """.trimIndent(),
            listOf("source", "channel", Long.MAX_VALUE, Long.MIN_VALUE, 10L),
        )
    }

    private fun explain(sql: String, arguments: List<Any>): List<String> =
        driver.executeQuery(
            identifier = null,
            sql = sql,
            mapper = { cursor ->
                val details = mutableListOf<String>()
                while (cursor.next().value) details += checkNotNull(cursor.getString(3))
                QueryResult.Value(details)
            },
            parameters = arguments.size,
            binders = {
                arguments.forEachIndexed { index, value ->
                    when (value) {
                        is String -> bindString(index, value)
                        is Long -> bindLong(index, value)
                        else -> error("Unsupported query-plan argument")
                    }
                }
            },
        ).value

    private fun requireStaging(generation: CatalogGeneration) {
        if (!queries.generationExists(generation.sourceId.value, generation.value).executeAsOne()) {
            throw CatalogStoreException("Catalog generation is not writable")
        }
        val state = queries.sourceState(generation.sourceId.value).executeAsOneOrNull()
            ?: throw CatalogStoreException("Catalog generation is not writable")
        if (state.deleted != 0L || generation.value != state.next_generation - 1) {
            throw CatalogStoreException("Catalog generation is stale")
        }
        if (state.active_generation == generation.value) {
            throw CatalogStoreException("Active catalog generation is immutable")
        }
    }

    private fun generationCounts(generation: CatalogGeneration): CatalogGenerationCounts =
        CatalogGenerationCounts(
            catalogItems = queries.generationCatalogCount(
                generation.sourceId.value,
                generation.value,
            ).executeAsOne(),
            channels = queries.generationChannelCount(
                generation.sourceId.value,
                generation.value,
            ).executeAsOne(),
            programmes = queries.generationProgrammeCount(
                generation.sourceId.value,
                generation.value,
            ).executeAsOne(),
        )

    private fun generationRowCount(sourceId: String, generation: Long): Long =
        queries.generationRowCount(
            sourceId,
            generation,
            sourceId,
            generation,
            sourceId,
            generation,
        ).executeAsOne()

    private fun deleteGenerationIfEmpty(sourceId: String, generation: Long) {
        queries.deleteEmptyGeneration(
            sourceId,
            generation,
            sourceId,
            generation,
            sourceId,
            generation,
            sourceId,
            generation,
        )
    }

    private suspend fun <T> read(block: () -> T): T = withContext(databaseDispatcher) {
        currentCoroutineContext().ensureActive()
        try {
            block()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: IllegalArgumentException) {
            throw failure
        } catch (failure: CatalogStoreException) {
            throw failure
        } catch (failure: Throwable) {
            throw CatalogStoreException("Catalog storage operation failed", failure)
        }
    }

    private suspend fun <T> serializedWrite(block: () -> T): T =
        writer.withLock { read(block) }

    private suspend fun <T> probedWrite(block: (kotlin.coroutines.CoroutineContext) -> T): T =
        writer.withLock {
            withContext(databaseDispatcher) {
                val context = currentCoroutineContext()
                context.ensureActive()
                try {
                    block(context)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: IllegalArgumentException) {
                    throw failure
                } catch (failure: CatalogStoreException) {
                    throw failure
                } catch (failure: Throwable) {
                    throw CatalogStoreException("Catalog storage operation failed", failure)
                }
            }
        }
}

private data class EncodedCatalogItem(
    val entityId: String,
    val displayName: String,
    val payload: String,
)

private data class EncodedChannel(
    val id: String,
    val displayName: String,
    val payload: String,
)

private data class EncodedProgramme(
    val channelId: String,
    val startMs: Long,
    val endMs: Long,
    val eventId: String,
    val payload: String,
)

private val catalogJson = Json {
    encodeDefaults = true
    explicitNulls = true
    ignoreUnknownKeys = false
}

private fun requireBatch(size: Int) {
    require(size in 1..MAX_BATCH_ITEMS) { "Catalog batches are limited to $MAX_BATCH_ITEMS items" }
}

private fun requireItemMatches(kind: DurableCatalogKind, item: DurableCatalogItem) {
    val matches = when (kind) {
        DurableCatalogKind.Stremio -> item is DurableCatalogItem.Stremio
        DurableCatalogKind.IptvChannel -> item is DurableCatalogItem.IptvChannel
        DurableCatalogKind.IptvMovie -> item is DurableCatalogItem.IptvMovie
        DurableCatalogKind.IptvSeries -> item is DurableCatalogItem.IptvSeries
        DurableCatalogKind.IptvEpisode -> item is DurableCatalogItem.IptvEpisode
    }
    require(matches) { "Catalog item kind does not match its catalog" }
}

private fun encodeCatalogItem(item: DurableCatalogItem): EncodedCatalogItem = when (item) {
    is DurableCatalogItem.Stremio -> {
        validateMeta(item.value)
        EncodedCatalogItem(
            item.value.id.checkedIdentity(),
            item.value.name.checkedDisplayName(),
            encode(MetaPreview.serializer(), item.value),
        )
    }
    is DurableCatalogItem.IptvChannel -> {
        validateChannelMetadata(item.value)
        EncodedCatalogItem(
            item.value.id.value.checkedIdentity(),
            item.value.name.checkedDisplayName(),
            encode(IptvChannelMetadata.serializer(), item.value),
        )
    }
    is DurableCatalogItem.IptvMovie -> {
        validateMovieMetadata(item.value)
        EncodedCatalogItem(
            item.value.id.value.checkedIdentity(),
            item.value.name.checkedDisplayName(),
            encode(IptvMovieMetadata.serializer(), item.value),
        )
    }
    is DurableCatalogItem.IptvSeries -> {
        validateSeries(item.value)
        EncodedCatalogItem(
            item.value.id.value.checkedIdentity(),
            item.value.name.checkedDisplayName(),
            encode(IptvSeriesMetadata.serializer(), item.value),
        )
    }
    is DurableCatalogItem.IptvEpisode -> {
        validateEpisodeMetadata(item.value)
        EncodedCatalogItem(
            item.value.id.value.checkedIdentity(),
            item.value.title.checkedDisplayName(),
            encode(IptvEpisodeMetadata.serializer(), item.value),
        )
    }
}

private fun encodeChannel(value: EpgChannel): EncodedChannel {
    require(value.displayNames.isNotEmpty()) { "EPG channel must have a display name" }
    value.iconUrl?.requireSafeReference()
    value.urls.forEach(String::requireSafeReference)
    return EncodedChannel(
        value.id.value.checkedIdentity(),
        value.displayNames.first().checkedDisplayName(),
        encode(EpgChannel.serializer(), value),
    )
}

private fun encodeProgramme(value: DurableGuideProgramme): EncodedProgramme {
    val programme = value.programme
    val end = programme.end ?: throw IllegalArgumentException("EPG programme must have an end time")
    val startMs = programme.start.toEpochMilliseconds()
    val endMs = end.toEpochMilliseconds()
    require(endMs > startMs) { "EPG programme end must be after its start" }
    programme.iconUrl?.requireSafeReference()
    programme.channelId.value.checkedIdentity()
    programme.title.checkedDisplayName()
    return EncodedProgramme(
        channelId = programme.channelId.value,
        startMs = startMs,
        endMs = endMs,
        eventId = value.providerEventId,
        payload = encode(EpgProgramme.serializer(), programme),
    )
}

private fun validateMeta(value: MetaPreview) {
    value.id.checkedIdentity()
    value.name.checkedDisplayName()
    value.poster?.requireSafeReference()
    value.background?.requireSafeReference()
    value.links.forEach { it.url.requireSafeReference() }
    value.trailers.forEach { it.source.requireSafeReference() }
}

private fun validateChannelMetadata(value: IptvChannelMetadata) {
    value.id.value.checkedIdentity()
    value.name.checkedDisplayName()
    value.logoUrl?.requireSafeReference()
}

private fun validateMovieMetadata(value: IptvMovieMetadata) {
    value.id.value.checkedIdentity()
    value.name.checkedDisplayName()
    value.posterUrl?.requireSafeReference()
}

private fun validateSeries(value: IptvSeriesMetadata) {
    value.id.value.checkedIdentity()
    value.name.checkedDisplayName()
    value.coverUrl?.requireSafeReference()
}

private fun validateEpisodeMetadata(value: IptvEpisodeMetadata) {
    value.id.value.checkedIdentity()
    value.seriesId.value.checkedIdentity()
    value.title.checkedDisplayName()
}

private fun String.checkedIdentity(): String {
    require(isNotBlank() && length <= 512 && '\u0000' !in this) { "Catalog identity is invalid" }
    return this
}

private fun String.checkedDisplayName(): String {
    require(isNotBlank() && length <= 2_048 && '\u0000' !in this) { "Display name is invalid" }
    return this
}

private fun String.requireSafeReference() {
    val value = trim()
    require(value.isNotEmpty() && value.length <= 8_192) { "Media reference is invalid" }
    val lower = value.lowercase()
    require(!lower.startsWith("data:") && !lower.startsWith("blob:")) {
        "Inline media bytes are not accepted"
    }
    val schemeEnd = lower.indexOf("://")
    if (schemeEnd >= 0) {
        val authorityEnd = lower.indexOfAny(charArrayOf('/', '?', '#'), schemeEnd + 3)
            .let { if (it == -1) lower.length else it }
        require('@' !in lower.substring(schemeEnd + 3, authorityEnd)) {
            "Credential-bearing media references are not accepted"
        }
    }
    val query = lower.substringAfter('?', "").substringBefore('#')
    if (query.isNotEmpty()) {
        val forbidden = setOf(
            "username", "user", "password", "pass", "token", "access_token",
            "auth", "authorization", "api_key", "apikey", "key",
        )
        require(query.split('&').none { part -> part.substringBefore('=').trim() in forbidden }) {
            "Credential-bearing media references are not accepted"
        }
    }
}

private fun <T> encode(serializer: KSerializer<T>, value: T): String {
    val payload = catalogJson.encodeToString(serializer, value)
    require(payload.encodeToByteArray().size <= MAX_PAYLOAD_BYTES) {
        "Catalog payload exceeds the storage limit"
    }
    return payload
}

private fun decodeCatalogItem(kind: DurableCatalogKind, payload: String): DurableCatalogItem =
    when (kind) {
        DurableCatalogKind.Stremio ->
            DurableCatalogItem.Stremio(decode(MetaPreview.serializer(), payload))
        DurableCatalogKind.IptvChannel ->
            DurableCatalogItem.IptvChannel(decode(IptvChannelMetadata.serializer(), payload))
        DurableCatalogKind.IptvMovie ->
            DurableCatalogItem.IptvMovie(decode(IptvMovieMetadata.serializer(), payload))
        DurableCatalogKind.IptvSeries ->
            DurableCatalogItem.IptvSeries(decode(IptvSeriesMetadata.serializer(), payload))
        DurableCatalogKind.IptvEpisode ->
            DurableCatalogItem.IptvEpisode(decode(IptvEpisodeMetadata.serializer(), payload))
    }

private fun <T> decode(serializer: KSerializer<T>, payload: String): T {
    if (payload.encodeToByteArray().size > MAX_PAYLOAD_BYTES) {
        throw CatalogStoreException("Stored catalog payload exceeds the decode limit")
    }
    return try {
        catalogJson.decodeFromString(serializer, payload)
    } catch (failure: SerializationException) {
        throw CatalogStoreException("Stored catalog payload is invalid", failure)
    }
}

private fun requirePayloadVersion(value: Long) {
    if (value != PAYLOAD_VERSION) {
        throw CatalogStoreException("Stored catalog payload version is unsupported")
    }
}

private fun com.getair.core.catalog.db.SourceState.toStatus(): CatalogSourceStatus =
    CatalogSourceStatus(
        activeGeneration = active_generation,
        revision = revision,
        activatedAt = activated_at_ms?.let(Instant::fromEpochMilliseconds),
    )
