package com.getair.core.catalog

// Shared browser policy; raw DOM/Promise interop remains in the JS and WasmJS leaves.

import com.getair.core.source.LocalSourceId
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgNowNext
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.IptvChannelMetadata
import com.getair.iptv.model.IptvEpisodeMetadata
import com.getair.iptv.model.IptvMovieMetadata
import com.getair.iptv.model.IptvPlaylistEntryMetadata
import com.getair.iptv.model.IptvSeriesMetadata
import com.getair.stremio.model.MetaPreview
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put

private const val INDEXED_DB_PAYLOAD_VERSION = 1L

/** A platform bridge whose only responsibility is executing the embedded IndexedDB runtime. */
internal interface BrowserIndexedDbExecutor {
    suspend fun execute(
        databaseName: String,
        operationId: String,
        commandJson: String,
    ): String

    fun close(databaseName: String)
}

internal class BrowserIndexedDbFailure(val code: String, cause: Throwable? = null) :
    IllegalStateException(code, cause)

internal interface IndexedDbMigrationTestAccess {
    suspend fun awaitGuideMigrationForTest(): Int
}

internal suspend fun openIndexedDbDurableCatalogStore(
    databaseName: String,
    executor: BrowserIndexedDbExecutor,
    options: CatalogStorageOptions = CatalogStorageOptions(),
    nowMillis: () -> Long = { Clock.System.now().toEpochMilliseconds() },
    cleanupGuidesOnStartup: Boolean = true,
): DurableCatalogStore {
    require(databaseName.isNotBlank() && databaseName.length <= 128 && '\u0000' !in databaseName) {
        "IndexedDB database name must be between 1 and 128 characters"
    }
    return IndexedDbDurableCatalogStore(databaseName, executor, nowMillis).also { store ->
        store.initialize(options.startupCleanupRows, cleanupGuidesOnStartup)
    }
}

private class IndexedDbDurableCatalogStore(
    private val databaseName: String,
    private val executor: BrowserIndexedDbExecutor,
    private val nowMillis: () -> Long,
) : DurableCatalogStore, IndexedDbMigrationTestAccess {
    private val writes = Mutex()
    private var nextOperation = 0L
    private var closed = false
    private val migrationScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var migration: Deferred<Int>? = null
    private val guideStore = IndexedDbDurableGuideStore(databaseName, executor, nowMillis)
    override val guides: DurableGuideStore = guideStore

    suspend fun initialize(startupCleanupRows: Int, cleanupGuidesOnStartup: Boolean) {
        command("open") { }
        cleanupUnreachable(startupCleanupRows)
        if (cleanupGuidesOnStartup) guides.cleanupUnreachable(startupCleanupRows)
        migration = migrationScope.async {
            var batches = 0
            var hasMore: Boolean
            yield()
            do {
                val result = guideStore.migrateLegacyRows(startupCleanupRows)
                batches += 1
                hasMore = result.hasMore
                if (hasMore) yield()
            } while (hasMore)
            batches
        }
    }

    override suspend fun awaitGuideMigrationForTest(): Int = migration?.await() ?: 0

    override suspend fun beginRefresh(sourceId: LocalSourceId): CatalogGeneration = writes.withLock {
        val source = sourceId.component()
        val now = nowMillis()
        val result = command("begin") {
            put("sourceKey", sourceStateKey(source))
            put("sourceComponent", source)
            put("nowMs", now)
            // The runtime allocates the generation. Its queue/generation keys are supplied as prefixes.
            put("generationPrefix", generationPrefix(source))
            put("queuePrefix", cleanupQueuePrefix(now, source))
        }
        val generation = result.jsonObject.requiredLong("generation")
        CatalogGeneration(sourceId, generation)
    }

    override suspend fun stageCatalogBatch(
        generation: CatalogGeneration,
        catalog: DurableCatalogKey,
        items: List<DurableCatalogItem>,
    ) {
        require(items.size in 1..MAX_BATCH_ITEMS) {
            "Catalog batches are limited to $MAX_BATCH_ITEMS items"
        }
        items.forEach { requireIndexedDbItemMatches(catalog.kind, it) }
        val encoded = items.map(::encodeIndexedDbCatalogItem)
        writes.withLock {
            val source = generation.sourceId.component()
            val generationKey = positiveKey(generation.value)
            val kind = catalog.kind.storageValue.component()
            val catalogId = catalog.catalogId.component()
            command("stageCatalog") {
                put("sourceKey", sourceStateKey(source))
                put("generation", generation.value)
                put("generationKey", generationRecordKey(source, generationKey))
                put("counterKey", "K|$source|$generationKey|C|$kind|$catalogId")
                put("orderPrefix", "C|$source|$generationKey|$kind|$catalogId|")
                putJsonArray("rows", encoded) { item ->
                    val entity = item.entityId.component()
                    buildJsonObject {
                        put("recordKey", "I|$source|$generationKey|$kind|$catalogId|$entity")
                        put("entityKey", entity)
                        put("payloadVersion", INDEXED_DB_PAYLOAD_VERSION)
                        put("payload", item.payload)
                    }
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
        val encodedChannels = channels.map(::encodeIndexedDbChannel)
        val encodedProgrammes = programmes.map(::encodeIndexedDbProgramme)
        writes.withLock {
            val source = generation.sourceId.component()
            val generationKey = positiveKey(generation.value)
            command("stageGuide") {
                put("sourceKey", sourceStateKey(source))
                put("generation", generation.value)
                put("generationKey", generationRecordKey(source, generationKey))
                put("counterKey", "K|$source|$generationKey|H")
                put("orderPrefix", "H|$source|$generationKey|")
                putJsonArray("channels", encodedChannels) { channel ->
                    val channelKey = channel.id.component()
                    buildJsonObject {
                        put("recordKey", "J|$source|$generationKey|$channelKey")
                        put("channelKey", channelKey)
                        put("payloadVersion", INDEXED_DB_PAYLOAD_VERSION)
                        put("payload", channel.payload)
                    }
                }
                putJsonArray("programmes", encodedProgrammes) { programme ->
                    val channelKey = programme.channelId.component()
                    val eventKey = programme.eventId.component()
                    buildJsonObject {
                        put(
                            "recordKey",
                            "P|$source|$generationKey|$channelKey|${signedKey(programme.startMs)}|$eventKey",
                        )
                        put("eventId", programme.eventId)
                        put("startMs", programme.startMs)
                        put("endMs", programme.endMs)
                        put("payloadVersion", INDEXED_DB_PAYLOAD_VERSION)
                        put("payload", programme.payload)
                    }
                }
            }
        }
    }

    override suspend fun activate(
        generation: CatalogGeneration,
        expected: CatalogGenerationCounts,
    ): CatalogSourceStatus = writes.withLock {
        val source = generation.sourceId.component()
        val generationKey = positiveKey(generation.value)
        val now = nowMillis()
        command("activate") {
            put("sourceKey", sourceStateKey(source))
            put("sourceComponent", source)
            put("generation", generation.value)
            put("generationKey", generationRecordKey(source, generationKey))
            put("generationPrefix", generationPrefix(source))
            put("nowMs", now)
            put("nowKey", signedKey(now))
            put("expected", buildJsonObject {
                put("catalogItems", expected.catalogItems)
                put("channels", expected.channels)
                put("programmes", expected.programmes)
            })
            put("prefixes", generationPrefixes(source, generationKey))
        }.toSourceStatus()
    }

    override suspend fun sourceStatus(sourceId: LocalSourceId): CatalogSourceStatus? {
        val result = command("status") { put("sourceKey", sourceStateKey(sourceId.component())) }
        return if (result is JsonNull) null else result.toSourceStatus()
    }

    override suspend fun catalogPage(
        sourceId: LocalSourceId,
        catalog: DurableCatalogKey,
        afterSortOrder: Long?,
        limit: Int,
    ): DurableCatalogPage {
        require(limit in 1..MAX_CATALOG_PAGE_ITEMS) {
            "Catalog page limit must be between 1 and $MAX_CATALOG_PAGE_ITEMS"
        }
        val source = sourceId.component()
        val result = command("catalogPage") {
            put("sourceKey", sourceStateKey(source))
            put("activePrefix", "C|$source|")
            put("tailPrefix", "|${catalog.kind.storageValue.component()}|${catalog.catalogId.component()}|")
            put("afterKey", afterSortOrder?.let(::positiveKey))
            put("limit", limit)
        }.jsonArray
        val items = result.map { rowElement ->
            val row = rowElement.jsonObject
            row.requirePayloadVersion()
            DurableCatalogPageItem(
                sortOrder = row.requiredLong("sortOrder"),
                item = decodeIndexedDbCatalogItem(catalog.kind, row.requiredString("payload")),
            )
        }
        return DurableCatalogPage(items, items.lastOrNull()?.sortOrder?.takeIf { items.size == limit })
    }

    override suspend fun channelPage(
        sourceId: LocalSourceId,
        afterSortOrder: Long?,
        limit: Int,
    ): DurableChannelPage {
        require(limit in 1..MAX_CHANNEL_PAGE_ITEMS) {
            "Channel page limit must be between 1 and $MAX_CHANNEL_PAGE_ITEMS"
        }
        val source = sourceId.component()
        val result = command("channelPage") {
            put("sourceKey", sourceStateKey(source))
            put("activePrefix", "H|$source|")
            put("tailPrefix", "|")
            put("afterKey", afterSortOrder?.let(::positiveKey))
            put("limit", limit)
        }.jsonArray
        val items = result.map { rowElement ->
            val row = rowElement.jsonObject
            row.requirePayloadVersion()
            DurableChannelPageItem(
                sortOrder = row.requiredLong("sortOrder"),
                channel = decodeIndexedDb(EpgChannel.serializer(), row.requiredString("payload")),
            )
        }
        return DurableChannelPage(items, items.lastOrNull()?.sortOrder?.takeIf { items.size == limit })
    }

    override suspend fun guideWindow(
        sourceId: LocalSourceId,
        channelId: EpgChannelId,
        from: Instant,
        until: Instant,
        limit: Int,
    ): List<DurableGuideProgramme> {
        val fromMs = from.toEpochMilliseconds()
        val untilMs = until.toEpochMilliseconds()
        require(untilMs - fromMs in 1..MAX_GUIDE_WINDOW_MILLIS) {
            "Guide window must be positive and no longer than 14 days"
        }
        require(limit in 1..MAX_GUIDE_WINDOW_ITEMS) {
            "Guide window limit must be between 1 and $MAX_GUIDE_WINDOW_ITEMS"
        }
        val source = sourceId.component()
        return command("guideWindow") {
            put("sourceKey", sourceStateKey(source))
            put("activePrefix", "P|$source|")
            put("tailPrefix", "|${channelId.value.component()}|")
            put("fromMs", fromMs)
            put("untilKey", signedKey(untilMs))
            put("limit", limit)
        }.jsonArray.map { it.jsonObject.toGuideProgramme() }
    }

    override suspend fun nowNext(
        sourceId: LocalSourceId,
        channelId: EpgChannelId,
        at: Instant,
    ): EpgNowNext {
        val source = sourceId.component()
        val atMs = at.toEpochMilliseconds()
        val result = command("nowNext") {
            put("sourceKey", sourceStateKey(source))
            put("activePrefix", "P|$source|")
            put("tailPrefix", "|${channelId.value.component()}|")
            put("atMs", atMs)
            put("atKey", signedKey(atMs))
        }.jsonObject
        return EpgNowNext(
            current = result["current"]?.takeUnless { it is JsonNull }
                ?.jsonObject?.decodeProgramme(),
            next = result["next"]?.takeUnless { it is JsonNull }
                ?.jsonObject?.decodeProgramme(),
        )
    }

    override suspend fun deleteSource(sourceId: LocalSourceId) {
        writes.withLock {
            val source = sourceId.component()
            val now = nowMillis()
            command("deleteSource") {
                put("sourceKey", sourceStateKey(source))
                put("sourceComponent", source)
                put("generationPrefix", generationPrefix(source))
                put("nowKey", signedKey(now))
            }
        }
    }

    override suspend fun cleanupUnreachable(maxRows: Int): CatalogCleanupResult = writes.withLock {
        require(maxRows in 1..10_000) { "Cleanup row limit must be between 1 and 10000" }
        val result = command("cleanup") { put("maxRows", maxRows) }.jsonObject
        CatalogCleanupResult(
            removedRows = result.requiredLong("removedRows").toInt(),
            hasMore = result.requiredBoolean("hasMore"),
        )
    }

    override fun close() {
        if (!closed) {
            closed = true
            migrationScope.cancel()
            executor.close(databaseName)
        }
    }

    private suspend fun command(
        operation: String,
        content: kotlinx.serialization.json.JsonObjectBuilder.() -> Unit,
    ): JsonElement {
        check(!closed) { "Catalog store is closed" }
        val operationId = "air-idb-${++nextOperation}"
        val command = buildJsonObject {
            put("op", operation)
            put("operationId", operationId)
            content()
        }
        return try {
            indexedDbJson.parseToJsonElement(
                executor.execute(databaseName, operationId, indexedDbJson.encodeToString(JsonObject.serializer(), command)),
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: IllegalArgumentException) {
            throw failure
        } catch (failure: BrowserIndexedDbFailure) {
            throw failure.toCatalogStoreException()
        } catch (failure: CatalogStoreException) {
            throw failure
        } catch (failure: Throwable) {
            throw CatalogStoreException("Browser catalog storage operation failed", failure)
        }
    }
}

private data class IndexedDbEncodedCatalogItem(
    val entityId: String,
    val payload: String,
)

private data class IndexedDbEncodedChannel(
    val id: String,
    val payload: String,
)

private data class IndexedDbEncodedProgramme(
    val channelId: String,
    val startMs: Long,
    val endMs: Long,
    val eventId: String,
    val payload: String,
)

private val indexedDbJson = Json {
    encodeDefaults = true
    explicitNulls = true
    ignoreUnknownKeys = false
}

private fun requireIndexedDbItemMatches(kind: DurableCatalogKind, item: DurableCatalogItem) {
    val matches = when (kind) {
        DurableCatalogKind.Stremio -> item is DurableCatalogItem.Stremio
        DurableCatalogKind.IptvChannel -> item is DurableCatalogItem.IptvChannel
        DurableCatalogKind.IptvMovie -> item is DurableCatalogItem.IptvMovie
        DurableCatalogKind.IptvSeries -> item is DurableCatalogItem.IptvSeries
        DurableCatalogKind.IptvEpisode -> item is DurableCatalogItem.IptvEpisode
        DurableCatalogKind.M3uPlaylistEntry -> item is DurableCatalogItem.M3uPlaylistEntry
    }
    require(matches) { "Catalog item kind does not match its catalog" }
}

private fun encodeIndexedDbCatalogItem(item: DurableCatalogItem): IndexedDbEncodedCatalogItem =
    when (item) {
        is DurableCatalogItem.Stremio -> {
            validateIndexedDbMeta(item.value)
            IndexedDbEncodedCatalogItem(
                item.value.id.checkedIndexedDbIdentity(),
                encodeIndexedDb(MetaPreview.serializer(), item.value),
            )
        }
        is DurableCatalogItem.IptvChannel -> {
            item.value.id.value.checkedIndexedDbIdentity()
            item.value.name.checkedIndexedDbDisplayName()
            item.value.logoUrl?.requireIndexedDbSafeReference()
            IndexedDbEncodedCatalogItem(
                item.value.id.value,
                encodeIndexedDb(IptvChannelMetadata.serializer(), item.value),
            )
        }
        is DurableCatalogItem.IptvMovie -> {
            item.value.id.value.checkedIndexedDbIdentity()
            item.value.name.checkedIndexedDbDisplayName()
            item.value.posterUrl?.requireIndexedDbSafeReference()
            IndexedDbEncodedCatalogItem(
                item.value.id.value,
                encodeIndexedDb(IptvMovieMetadata.serializer(), item.value),
            )
        }
        is DurableCatalogItem.IptvSeries -> {
            item.value.id.value.checkedIndexedDbIdentity()
            item.value.name.checkedIndexedDbDisplayName()
            item.value.coverUrl?.requireIndexedDbSafeReference()
            IndexedDbEncodedCatalogItem(
                item.value.id.value,
                encodeIndexedDb(IptvSeriesMetadata.serializer(), item.value),
            )
        }
        is DurableCatalogItem.IptvEpisode -> {
            item.value.id.value.checkedIndexedDbIdentity()
            item.value.seriesId.value.checkedIndexedDbIdentity()
            item.value.title.checkedIndexedDbDisplayName()
            IndexedDbEncodedCatalogItem(
                item.value.id.value,
                encodeIndexedDb(IptvEpisodeMetadata.serializer(), item.value),
            )
        }
        is DurableCatalogItem.M3uPlaylistEntry -> {
            item.value.id.value.checkedIndexedDbIdentity()
            item.value.name.checkedIndexedDbDisplayName()
            item.value.logoUrl?.requireIndexedDbSafeReference()
            IndexedDbEncodedCatalogItem(
                item.value.id.value,
                encodeIndexedDb(IptvPlaylistEntryMetadata.serializer(), item.value),
            )
        }
    }

private fun encodeIndexedDbChannel(value: EpgChannel): IndexedDbEncodedChannel {
    require(value.displayNames.isNotEmpty()) { "EPG channel must have a display name" }
    value.id.value.checkedIndexedDbIdentity()
    value.displayNames.first().checkedIndexedDbDisplayName()
    value.iconUrl?.requireIndexedDbSafeReference()
    value.urls.forEach(String::requireIndexedDbSafeReference)
    return IndexedDbEncodedChannel(value.id.value, encodeIndexedDb(EpgChannel.serializer(), value))
}

private fun encodeIndexedDbProgramme(value: DurableGuideProgramme): IndexedDbEncodedProgramme {
    val programme = value.programme
    val end = programme.end ?: throw IllegalArgumentException("EPG programme must have an end time")
    val startMs = programme.start.toEpochMilliseconds()
    val endMs = end.toEpochMilliseconds()
    require(endMs > startMs) { "EPG programme end must be after its start" }
    programme.channelId.value.checkedIndexedDbIdentity()
    programme.title.checkedIndexedDbDisplayName()
    programme.iconUrl?.requireIndexedDbSafeReference()
    return IndexedDbEncodedProgramme(
        programme.channelId.value,
        startMs,
        endMs,
        value.providerEventId,
        encodeIndexedDb(EpgProgramme.serializer(), programme),
    )
}

private fun validateIndexedDbMeta(value: MetaPreview) {
    value.id.checkedIndexedDbIdentity()
    value.name.checkedIndexedDbDisplayName()
    value.poster?.requireIndexedDbSafeReference()
    value.background?.requireIndexedDbSafeReference()
    value.links.forEach { it.url.requireIndexedDbSafeReference() }
    value.trailers.forEach { it.source.requireIndexedDbSafeReference() }
}

private fun String.checkedIndexedDbIdentity(): String {
    require(isNotBlank() && length <= 512 && '\u0000' !in this) { "Catalog identity is invalid" }
    return this
}

private fun String.checkedIndexedDbDisplayName(): String {
    require(isNotBlank() && length <= 2_048 && '\u0000' !in this) { "Display name is invalid" }
    return this
}

private fun String.requireIndexedDbSafeReference() {
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
        require(query.split('&').none { it.substringBefore('=').trim() in forbidden }) {
            "Credential-bearing media references are not accepted"
        }
    }
}

private fun <T> encodeIndexedDb(serializer: KSerializer<T>, value: T): String {
    val payload = indexedDbJson.encodeToString(serializer, value)
    require(payload.encodeToByteArray().size <= MAX_PAYLOAD_BYTES) {
        "Catalog payload exceeds the storage limit"
    }
    return payload
}

private fun decodeIndexedDbCatalogItem(kind: DurableCatalogKind, payload: String): DurableCatalogItem =
    when (kind) {
        DurableCatalogKind.Stremio ->
            DurableCatalogItem.Stremio(decodeIndexedDb(MetaPreview.serializer(), payload))
        DurableCatalogKind.IptvChannel ->
            DurableCatalogItem.IptvChannel(decodeIndexedDb(IptvChannelMetadata.serializer(), payload))
        DurableCatalogKind.IptvMovie ->
            DurableCatalogItem.IptvMovie(decodeIndexedDb(IptvMovieMetadata.serializer(), payload))
        DurableCatalogKind.IptvSeries ->
            DurableCatalogItem.IptvSeries(decodeIndexedDb(IptvSeriesMetadata.serializer(), payload))
        DurableCatalogKind.IptvEpisode ->
            DurableCatalogItem.IptvEpisode(decodeIndexedDb(IptvEpisodeMetadata.serializer(), payload))
        DurableCatalogKind.M3uPlaylistEntry ->
            DurableCatalogItem.M3uPlaylistEntry(decodeIndexedDb(IptvPlaylistEntryMetadata.serializer(), payload))
    }

private fun <T> decodeIndexedDb(serializer: KSerializer<T>, payload: String): T {
    if (payload.encodeToByteArray().size > MAX_PAYLOAD_BYTES) {
        throw CatalogStoreException("Stored catalog payload exceeds the decode limit")
    }
    return try {
        indexedDbJson.decodeFromString(serializer, payload)
    } catch (failure: SerializationException) {
        throw CatalogStoreException("Stored catalog payload is invalid", failure)
    }
}

private fun JsonObject.toGuideProgramme(): DurableGuideProgramme {
    requirePayloadVersion()
    return DurableGuideProgramme(requiredString("eventId"), decodeProgramme())
}

private fun JsonObject.decodeProgramme(): EpgProgramme {
    requirePayloadVersion()
    return decodeIndexedDb(EpgProgramme.serializer(), requiredString("payload"))
}

private fun JsonObject.requirePayloadVersion() {
    if (requiredLong("payloadVersion") != INDEXED_DB_PAYLOAD_VERSION) {
        throw CatalogStoreException("Stored catalog payload version is unsupported")
    }
}

private fun JsonElement.toSourceStatus(): CatalogSourceStatus {
    val value = jsonObject
    return CatalogSourceStatus(
        activeGeneration = value["activeGeneration"]?.jsonPrimitive?.longOrNull,
        revision = value.requiredLong("revision"),
        activatedAt = value["activatedAtMs"]?.jsonPrimitive?.longOrNull
            ?.let(Instant::fromEpochMilliseconds),
    )
}

private fun JsonObject.requiredString(name: String): String =
    this[name]?.jsonPrimitive?.contentOrNull
        ?: throw CatalogStoreException("Stored browser catalog metadata is invalid")

private fun JsonObject.requiredLong(name: String): Long =
    this[name]?.jsonPrimitive?.longOrNull
        ?: throw CatalogStoreException("Stored browser catalog metadata is invalid")

private fun JsonObject.requiredBoolean(name: String): Boolean = try {
    this[name]?.jsonPrimitive?.boolean
        ?: throw CatalogStoreException("Stored browser catalog metadata is invalid")
} catch (failure: IllegalArgumentException) {
    throw CatalogStoreException("Stored browser catalog metadata is invalid", failure)
}

private fun kotlinx.serialization.json.JsonObjectBuilder.put(name: String, value: String?) {
    put(name, value?.let(::JsonPrimitive) ?: JsonNull)
}

private fun <T> kotlinx.serialization.json.JsonObjectBuilder.putJsonArray(
    name: String,
    values: List<T>,
    transform: (T) -> JsonElement,
) {
    put(name, buildJsonArray { values.forEach { add(transform(it)) } })
}

private fun LocalSourceId.component(): String = value.component()

private fun String.component(): String = buildString(length * 2) {
    encodeToByteArray().forEach { byte ->
        val value = byte.toInt() and 0xff
        append(HEX_DIGITS[value ushr 4])
        append(HEX_DIGITS[value and 0x0f])
    }
}

private fun positiveKey(value: Long): String {
    require(value in 0..9_007_199_254_740_991L) { "Browser catalog counter is exhausted" }
    return value.toString().padStart(16, '0')
}

private fun signedKey(value: Long): String =
    (value xor Long.MIN_VALUE).toULong().toString(16).padStart(16, '0')

private fun sourceStateKey(source: String): String = "S|$source"
private fun generationPrefix(source: String): String = "G|$source|"
private fun generationRecordKey(source: String, generation: String): String = "G|$source|$generation"
private fun cleanupQueuePrefix(now: Long, source: String): String = "Q|${signedKey(now)}|$source|"

private fun generationPrefixes(source: String, generation: String): JsonObject = buildJsonObject {
    put("catalogRecord", "I|$source|$generation|")
    put("channelRecord", "J|$source|$generation|")
    put("programme", "P|$source|$generation|")
}

private fun BrowserIndexedDbFailure.toCatalogStoreException(): CatalogStoreException =
    CatalogStoreException(
        when (code) {
            "AIR_IDB_UNAVAILABLE" -> "IndexedDB is unavailable in this browser context"
            "AIR_IDB_BLOCKED" -> "IndexedDB upgrade is blocked by another open tab"
            "AIR_IDB_QUOTA" -> "Browser catalog storage quota was exceeded"
            "AIR_IDB_CANCELLED", "AIR_IDB_ABORT" -> "Browser catalog transaction was aborted"
            "AIR_IDB_STALE" -> "Catalog generation is stale"
            "AIR_IDB_NOT_WRITABLE" -> "Catalog generation is not writable"
            "AIR_IDB_ACTIVE_IMMUTABLE" -> "Active catalog generation is immutable"
            "AIR_IDB_COUNT_MISMATCH" -> "Staged catalog counts do not match"
            "AIR_IDB_GENERATION_EXHAUSTED" -> "Catalog generation space is exhausted"
            "AIR_IDB_CORRUPT" -> "Stored browser catalog metadata is invalid"
            else -> "Browser catalog storage operation failed"
        },
        this,
    )

private const val HEX_DIGITS = "0123456789abcdef"
