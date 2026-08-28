package com.getair.core.catalog

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import kotlinx.datetime.Instant
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlin.random.Random

internal interface IndexedDbGuideTestAccess {
    suspend fun rawGuideRecordsForTest(): List<String>
}

internal class IndexedDbDurableGuideStore(
    private val databaseName: String,
    private val executor: BrowserIndexedDbExecutor,
    private val nowMillis: () -> Long,
) : DurableGuideStore, IndexedDbGuideTestAccess {
    override val leaseIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_LEASE_IDLE_TIMEOUT_MILLIS
    override val generationIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_GENERATION_IDLE_TIMEOUT_MILLIS

    private val ownerId = randomOpaque("owner")
    private var operationCounter = 0L

    suspend fun migrateLegacyRows(maxRows: Int): DurableGuideCleanupResult {
        val result = command("guideMigrateLegacy") { put("maxRows", maxRows) }.objectResult()
        result.throwExpectedFailure()
        return DurableGuideCleanupResult(
            result.requiredLong("migratedRows").toInt(),
            result.requiredBoolean("hasMore"),
        )
    }

    override suspend fun beginRefresh(
        key: DurableGuideKey,
        retention: DurableGuideRetention,
    ): DurableGuideGeneration {
        val keys = GuideKeys(key)
        val result = command("guideBegin") {
            putKeys(keys)
            put("retention", retention.toJson())
            put("nowMs", nowMillis())
            put("generationIdleTimeoutMillis", generationIdleTimeoutMillis)
        }.objectResult()
        result.requireStatus("ok")
        return Generation(
            ownerId = ownerId,
            key = key,
            value = result.requiredLong("generation"),
            mutationEpoch = result.requiredLong("mutationEpoch"),
            retention = retention,
        )
    }

    override suspend fun renewGeneration(generation: DurableGuideGeneration): Boolean {
        val token = generation.ownedGeneration() ?: throw DurableGuideStoreException.Stale()
        val result = command("guideRenewGeneration") {
            put("generationKey", GuideKeys(token.key).generationKey(token.value))
            put("nowMs", nowMillis())
            put("generationIdleTimeoutMillis", generationIdleTimeoutMillis)
        }.objectResult()
        return result.requiredBoolean("value")
    }

    override suspend fun abandon(generation: DurableGuideGeneration): Boolean {
        val token = generation.ownedGeneration() ?: throw DurableGuideStoreException.Stale()
        val result = command("guideAbandon") {
            put("generationKey", GuideKeys(token.key).generationKey(token.value))
            put("nowMs", nowMillis())
        }.objectResult()
        return result.requiredBoolean("value")
    }

    override suspend fun stage(
        generation: DurableGuideGeneration,
        channels: List<DurableGuideChannelRecord>,
        programmes: List<DurableGuideProgrammeRecord>,
    ): DurableGuideCounts {
        val token = generation.ownedGeneration() ?: throw DurableGuideStoreException.Stale()
        val inputRows = channels.size.toLong() + programmes.size.toLong()
        if (inputRows > DurableGuideLimits.MAX_BATCH_ITEMS) {
            val rejected = command("guideRejectStage") {
                put("generationKey", GuideKeys(token.key).generationKey(token.value))
                put("nowMs", nowMillis())
                put("inputChannelRows", channels.size)
                put("inputProgrammeRows", programmes.size)
            }.objectResult()
            rejected.throwExpectedFailure()
            throw DurableGuideStoreException.Corrupt()
        }
        programmes.forEach { candidate ->
            require(
                candidate.start < token.retention.retainedUntil &&
                    candidate.effectiveEnd > token.retention.retainedFrom,
            ) { "Programme lies outside the generation retention interval" }
        }
        val keys = GuideKeys(token.key)
        val generationComponent = positiveKey(token.value)
        val channelPrefix = keys.channelPrefix(generationComponent)
        val programmePrefix = keys.programmePrefix(generationComponent)
        val result = command("guideStage") {
            put("generationKey", keys.generationKey(token.value))
            put("nowMs", nowMillis())
            put("generationIdleTimeoutMillis", generationIdleTimeoutMillis)
            put("maxBatchItems", DurableGuideLimits.MAX_BATCH_ITEMS)
            put("maxBatches", DurableGuideLimits.MAX_GENERATION_BATCHES)
            put("maxInputChannels", DurableGuideLimits.MAX_INPUT_CHANNEL_ROWS)
            put("maxInputProgrammes", DurableGuideLimits.MAX_INPUT_PROGRAMME_ROWS)
            put("maxChannels", DurableGuideLimits.MAX_GENERATION_CHANNELS)
            put("maxProgrammes", DurableGuideLimits.MAX_GENERATION_PROGRAMMES)
            put("channels", buildJsonArray {
                channels.forEach { channel -> add(channel.toJson(channelPrefix)) }
            })
            put("programmes", buildJsonArray {
                programmes.forEach { programme -> add(programme.toJson(programmePrefix)) }
            })
        }.objectResult()
        result.throwExpectedFailure()
        val counts = result["counts"]?.jsonObject ?: throw DurableGuideStoreException.Corrupt()
        return DurableGuideCounts(counts.requiredLong("channels"), counts.requiredLong("programmes"))
    }

    override suspend fun activate(
        generation: DurableGuideGeneration,
        expected: DurableGuideCounts,
    ): DurableGuideActivation {
        val token = generation.ownedGeneration() ?: throw DurableGuideStoreException.Stale()
        val keys = GuideKeys(token.key)
        val result = command("guideActivate") {
            putKeys(keys)
            put("generationKey", keys.generationKey(token.value))
            put("expected", expected.toJson())
            put("nowMs", nowMillis())
        }.objectResult()
        return when (result.requiredString("status")) {
            "published" -> DurableGuideActivation.Published(
                result.requiredElement("snapshot").jsonObject.toSnapshot(ownerId),
            )
            "superseded" -> DurableGuideActivation.Superseded(result.optionalSnapshot("current", ownerId))
            "stale" -> throw DurableGuideStoreException.Stale()
            "limit" -> throw DurableGuideStoreException.Limit()
            else -> throw DurableGuideStoreException.Corrupt()
        }
    }

    override suspend fun snapshot(key: DurableGuideKey): DurableGuideSnapshot? {
        val keys = GuideKeys(key)
        val result = command("guideSnapshot") { putKeys(keys) }
        return if (result is JsonNull) null else result.jsonObject.toSnapshot(ownerId)
    }

    override suspend fun snapshots(
        source: DurableGuideSourceSnapshot,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideSnapshotPage {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = source.token as? SourceToken
        if (token == null || token.ownerId != ownerId || token.sourceKey != source.sourceKey) {
            throw DurableGuideStoreException.Stale()
        }
        val domain = "snapshots:${source.sourceKey.value}:${token.epoch}:${token.mutation}"
        val cursor = after.ownedCursor(domain)
        val result = command("guideSnapshots") {
            put("sourceStateKey", sourceStateKey(source.sourceKey))
            put("activeFeedPrefix", activeFeedPrefix(source.sourceKey, token.epoch))
            put("sourceEpoch", token.epoch)
            put("sourceMutation", token.mutation)
            put("afterKey", cursor?.afterKey)
            put("limit", limit)
        }.objectResult()
        result.throwExpectedFailure()
        val rows = result.requiredArray("rows").map { it.jsonObject.toSnapshot(ownerId) }
        return DurableGuideSnapshotPage(rows, result.optionalString("nextKey")?.let { Cursor(ownerId, domain, it) })
    }

    override suspend fun acquire(snapshot: DurableGuideSnapshot): DurableGuideSnapshotLease? {
        val token = snapshot as? Snapshot ?: return null
        if (token.ownerId != ownerId) return null
        val keys = GuideKeys(token.key)
        val leaseKey = "GL|${randomOpaque("lease")}"
        val result = command("guideAcquire") {
            put("generationKey", keys.generationKey(token.generation))
            put("generation", token.generation)
            put("mutationEpoch", token.mutationEpoch)
            put("leaseKey", leaseKey)
            put("ownerId", ownerId)
            put("nowMs", nowMillis())
            put("leaseIdleTimeoutMillis", leaseIdleTimeoutMillis)
            put("maxLiveLeases", DurableGuideLimits.MAX_LIVE_LEASES)
        }.objectResult()
        return when (result.requiredString("status")) {
            "ok" -> Lease(ownerId, leaseKey, token)
            "missing" -> null
            "limit" -> throw DurableGuideStoreException.Limit()
            else -> throw DurableGuideStoreException.Corrupt()
        }
    }

    override suspend fun renew(lease: DurableGuideSnapshotLease): Boolean {
        val token = lease as? Lease ?: return false
        if (token.ownerId != ownerId) return false
        val result = command("guideRenewLease") {
            put("leaseKey", token.leaseKey)
            put("ownerId", ownerId)
            put("nowMs", nowMillis())
            put("leaseIdleTimeoutMillis", leaseIdleTimeoutMillis)
        }.objectResult()
        return result.requiredBoolean("value")
    }

    override suspend fun release(lease: DurableGuideSnapshotLease) {
        val token = lease as? Lease ?: return
        if (token.ownerId != ownerId) return
        command("guideReleaseLease") {
            put("leaseKey", token.leaseKey)
            put("ownerId", ownerId)
        }.objectResult()
    }

    override suspend fun channels(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideChannelPage {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = lease.ownedLease()
        val keys = GuideKeys(token.snapshot.key)
        val domain = "channels:${token.leaseKey}"
        val cursor = after.ownedCursor(domain)
        val result = leasedCommand("guideChannels", token) {
            put("channelPrefix", keys.channelPrefix(positiveKey(token.snapshot.generation)))
            put("afterKey", cursor?.afterKey)
            put("limit", limit)
        }
        val rows = result.requiredArray("rows").map { it.jsonObject.toChannel() }
        return DurableGuideChannelPage(rows, result.optionalString("nextKey")?.let { Cursor(ownerId, domain, it) })
    }

    override suspend fun programmeSearchRows(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideProgrammeSearchPage {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = lease.ownedLease()
        val keys = GuideKeys(token.snapshot.key)
        val domain = "search:${token.leaseKey}"
        val cursor = after.ownedCursor(domain)
        val result = leasedCommand("guideSearchRows", token) {
            put("programmePrefix", keys.programmePrefix(positiveKey(token.snapshot.generation)))
            put("afterKey", cursor?.afterKey)
            put("limit", limit)
        }
        val rows = result.requiredArray("rows").map { element ->
            val row = element.jsonObject
            DurableGuideProgrammeSearchRow(
                locator = Locator(ownerId, keys.generationKey(token.snapshot.generation), row.requiredString("locatorKey")),
                start = row.requiredInstant("startMs"),
                effectiveEnd = row.requiredInstant("effectiveEndMs"),
                title = row.requiredString("title"),
                subtitle = row.optionalString("subtitle"),
            )
        }
        return DurableGuideProgrammeSearchPage(rows, result.optionalString("nextKey")?.let { Cursor(ownerId, domain, it) })
    }

    override suspend fun programme(
        lease: DurableGuideSnapshotLease,
        locator: DurableGuideProgrammeLocator,
    ): DurableGuideProgrammeRecord? {
        val token = lease as? Lease ?: return null
        val location = locator as? Locator ?: return null
        if (token.ownerId != ownerId || location.ownerId != ownerId || location.generationKey != GuideKeys(token.snapshot.key).generationKey(token.snapshot.generation)) return null
        val keys = GuideKeys(token.snapshot.key)
        val result = leasedCommand("guideProgramme", token, staleAsNull = true) {
            put("locatorGenerationKey", location.generationKey)
            put("locatorKey", location.recordKey)
            put("programmePrefix", keys.programmePrefix(positiveKey(token.snapshot.generation)))
        }
        if (result["status"]?.jsonPrimitive?.content == "stale") return null
        val row = result["row"]?.takeUnless { it is JsonNull }?.jsonObject ?: return null
        return row.toProgramme()
    }

    override suspend fun window(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideWindowPage {
        val token = lease.ownedLease()
        val domain = "window:${token.leaseKey}:${channelKey.value}:${from.toEpochMilliseconds()}:${until.toEpochMilliseconds()}"
        val cursor = after.ownedCursor(domain)
        val result = windowPage(token, channelKey, from, until, cursor?.afterKey, limit, DurableGuideLimits.MAX_MULTI_WINDOW_PAYLOAD_BYTES)
        val rows = result.requiredArray("rows").map { it.jsonObject.toProgramme() }
        return DurableGuideWindowPage(
            rows,
            result.optionalLong("nextStartMs")?.let { Cursor(ownerId, domain, it.toString()) },
            result.requiredBoolean("truncated"),
        )
    }

    override suspend fun nowNext(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        at: Instant,
    ): DurableGuideNowNext {
        val token = lease.ownedLease()
        val keys = GuideKeys(token.snapshot.key)
        val generation = positiveKey(token.snapshot.generation)
        val result = leasedCommand("guideNowNext", token) {
            put("channelProgrammePrefix", keys.programmePrefix(generation) + channelKey.value + "|")
            put("atMs", at.toEpochMilliseconds())
            put("atKey", signedKey(at.toEpochMilliseconds()))
        }
        return DurableGuideNowNext(result.optionalProgramme("current"), result.optionalProgramme("next"))
    }

    override suspend fun multiChannelWindow(
        lease: DurableGuideSnapshotLease,
        request: DurableGuideMultiChannelWindowRequest,
    ): DurableGuideMultiChannelWindow {
        val token = lease.ownedLease()
        var rowsRemaining = request.totalLimit
        var bytesRemaining = request.payloadByteLimit
        var payloadBytes = 0
        var truncated = false
        val channels = request.channelKeys.map { channelKey ->
            val rowBudgetExhausted = rowsRemaining == 0
            val result = windowPage(
                token,
                channelKey,
                request.from,
                request.until,
                afterKey = null,
                limit = minOf(request.perChannelLimit, rowsRemaining.coerceAtLeast(1)),
                payloadByteLimit = if (rowBudgetExhausted) 0 else bytesRemaining,
            )
            val rows = result.requiredArray("rows").map { it.jsonObject.toProgramme() }
            val used = result.requiredLong("payloadBytes").toInt()
            payloadBytes += used
            bytesRemaining -= used
            rowsRemaining -= rows.size
            val channelTruncated = result.requiredBoolean("truncated")
            truncated = truncated || channelTruncated
            DurableGuideChannelWindow(channelKey, rows, channelTruncated)
        }
        return DurableGuideMultiChannelWindow(channels, payloadBytes, truncated)
    }

    override suspend fun prune(
        key: DurableGuideKey,
        expectedRevision: Long,
        expectedMutationEpoch: Long,
        retention: DurableGuideRetention,
    ): DurableGuidePruneResult {
        val current = snapshot(key)
        if (current == null || current.revision != expectedRevision || current.mutationEpoch != expectedMutationEpoch) {
            return DurableGuidePruneResult.Superseded(current)
        }
        val lease = acquire(current) ?: return DurableGuidePruneResult.Superseded(snapshot(key))
        var generation: Generation? = null
        try {
            val keys = GuideKeys(key)
            val begin = command("guideBeginPrune") {
                putKeys(keys)
                put("expectedRevision", expectedRevision)
                put("expectedMutationEpoch", expectedMutationEpoch)
                put("retention", retention.toJson())
                put("nowMs", nowMillis())
                put("generationIdleTimeoutMillis", generationIdleTimeoutMillis)
            }.objectResult()
            when (begin.requiredString("status")) {
                "superseded" -> return DurableGuidePruneResult.Superseded(begin.optionalSnapshot("current", ownerId))
                "limit" -> throw DurableGuideStoreException.Limit()
                "ok" -> generation = Generation(
                    ownerId,
                    key,
                    begin.requiredLong("generation"),
                    begin.requiredLong("mutationEpoch"),
                    retention,
                )
                else -> throw DurableGuideStoreException.Corrupt()
            }
            var counts = DurableGuideCounts(0, 0)
            var channelCursor: DurableGuideCursor? = null
            do {
                if (!renew(lease)) throw DurableGuideStoreException.Stale()
                val page = channels(lease, channelCursor, DurableGuideLimits.MAX_PAGE_ITEMS)
                counts = stage(requireNotNull(generation), channels = page.channels)
                channelCursor = page.nextCursor
            } while (channelCursor != null)

            var programmeCursor: Cursor? = null
            do {
                if (!renew(lease)) throw DurableGuideStoreException.Stale()
                val page = fullProgrammePage(lease, programmeCursor, DurableGuideLimits.MAX_PAGE_ITEMS)
                val retained = page.rows.filter { it.start < retention.retainedUntil && it.effectiveEnd > retention.retainedFrom }
                if (retained.isNotEmpty()) counts = stage(requireNotNull(generation), programmes = retained)
                programmeCursor = page.nextCursor
            } while (programmeCursor != null)

            if (counts == current.counts) {
                val result = command("guideFinishPruneUnchanged") {
                    put("generationKey", keys.generationKey(requireNotNull(generation).value))
                }.objectResult()
                return when (result.requiredString("status")) {
                    "unchanged" -> DurableGuidePruneResult.Unchanged(result.requiredElement("current").jsonObject.toSnapshot(ownerId))
                    "superseded" -> DurableGuidePruneResult.Superseded(result.optionalSnapshot("current", ownerId))
                    else -> throw DurableGuideStoreException.Stale()
                }
            }
            return when (val activation = activate(requireNotNull(generation), counts)) {
                is DurableGuideActivation.Published -> DurableGuidePruneResult.Published(activation.snapshot)
                is DurableGuideActivation.Superseded -> DurableGuidePruneResult.Superseded(activation.current)
            }
        } finally {
            release(lease)
            generation?.let { token ->
                withContext(NonCancellable) { runCatching { abandon(token) } }
            }
        }
    }

    override suspend fun deleteGuide(
        key: DurableGuideKey,
        expectedRevision: Long?,
        expectedMutationEpoch: Long?,
    ): DurableGuideDeleteResult {
        if ((expectedRevision == null) != (expectedMutationEpoch == null)) throw DurableGuideStoreException.Stale()
        val keys = GuideKeys(key)
        val result = command("guideDelete") {
            putKeys(keys)
            put("conditional", expectedRevision != null)
            put("expectedRevision", expectedRevision)
            put("expectedMutationEpoch", expectedMutationEpoch)
        }.objectResult()
        return when (result.requiredString("status")) {
            "deleted" -> DurableGuideDeleteResult.Deleted(result.requiredLong("revision"))
            "superseded" -> DurableGuideDeleteResult.Superseded(result.optionalSnapshot("current", ownerId))
            else -> throw DurableGuideStoreException.Corrupt()
        }
    }

    override suspend fun sourceSnapshot(sourceKey: DurableGuideSourceKey): DurableGuideSourceSnapshot {
        val result = command("guideSourceSnapshot") {
            put("sourceStateKey", sourceStateKey(sourceKey))
            put("sourceKey", sourceKey.value)
        }.objectResult()
        return DurableGuideSourceSnapshot(
            sourceKey,
            result.requiredLong("feedCount").toInt(),
            SourceToken(ownerId, sourceKey, result.requiredLong("epoch"), result.requiredLong("mutation")),
        )
    }

    override suspend fun deleteSource(
        sourceKey: DurableGuideSourceKey,
        expected: DurableGuideSourceToken?,
    ): DurableGuideSourceDeleteResult {
        val token = expected as? SourceToken
        val ownedToken = token?.takeIf { it.ownerId == ownerId && it.sourceKey == sourceKey }
        val result = command("guideDeleteSource") {
            put("sourceStateKey", sourceStateKey(sourceKey))
            put("sourceKey", sourceKey.value)
            put("conditional", expected != null)
            put("sourceEpoch", ownedToken?.epoch ?: -1L)
            put("sourceMutation", ownedToken?.mutation ?: -1L)
        }.objectResult()
        return when (result.requiredString("status")) {
            "deleted" -> DurableGuideSourceDeleteResult.Deleted(
                result.requiredLong("activeFeedCount").toInt(),
                result.requiredLong("stagedOnlyFeedCount").toInt(),
            )
            "superseded" -> DurableGuideSourceDeleteResult.Superseded(
                result.requiredLong("activeFeedCount").toInt(),
                result.requiredLong("stagedOnlyFeedCount").toInt(),
            )
            else -> throw DurableGuideStoreException.Corrupt()
        }
    }

    override suspend fun cleanupUnreachable(maxRows: Int): DurableGuideCleanupResult {
        if (maxRows !in 1..DurableGuideLimits.MAX_CLEANUP_ROWS) throw DurableGuideStoreException.Limit()
        val result = command("guideCleanup") {
            put("maxRows", maxRows)
            put("nowMs", nowMillis())
        }.objectResult()
        result.throwExpectedFailure()
        return DurableGuideCleanupResult(result.requiredLong("removedRows").toInt(), result.requiredBoolean("hasMore"))
    }

    override suspend fun rawGuideRecordsForTest(): List<String> {
        val result = command("guideDebugDump") { put("limit", 512) }.objectResult()
        return result.requiredArray("records").map { it.jsonPrimitive.content }
    }

    private suspend fun windowPage(
        lease: Lease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        afterKey: String?,
        limit: Int,
        payloadByteLimit: Int,
    ): JsonObject {
        require(from < until)
        require(limit in 1..DurableGuideLimits.MAX_WINDOW_ITEMS)
        while (true) {
            val result = leasedCommand("durableGuideWindow", lease, allowNeedsMigration = true) {
                put("channelKey", channelKey.value)
                put("afterStartMs", afterKey?.toLongOrNull())
                put("fromMs", from.toEpochMilliseconds())
                put("untilMs", until.toEpochMilliseconds())
                put("limit", limit)
                put("payloadByteLimit", payloadByteLimit)
                put("maxIndexVisits", maxOf(1_024, limit * 16).coerceAtMost(16_000))
            }
            if (result["status"]?.jsonPrimitive?.content != "needsMigration") return result
            migrateLegacyRows(DurableGuideLimits.DEFAULT_CLEANUP_ROWS)
            yield()
        }
    }

    private data class FullProgrammePage(val rows: List<DurableGuideProgrammeRecord>, val nextCursor: Cursor?)

    private suspend fun fullProgrammePage(lease: DurableGuideSnapshotLease, after: Cursor?, limit: Int): FullProgrammePage {
        val token = lease.ownedLease()
        val keys = GuideKeys(token.snapshot.key)
        val domain = "full:${token.leaseKey}"
        if (after != null && (after.ownerId != ownerId || after.domain != domain)) throw DurableGuideStoreException.Stale()
        val result = leasedCommand("guideFullProgrammes", token) {
            put("programmePrefix", keys.programmePrefix(positiveKey(token.snapshot.generation)))
            put("afterKey", after?.afterKey)
            put("limit", limit)
        }
        return FullProgrammePage(
            result.requiredArray("rows").map { it.jsonObject.toProgramme() },
            result.optionalString("nextKey")?.let { Cursor(ownerId, domain, it) },
        )
    }

    private suspend fun leasedCommand(
        operation: String,
        lease: Lease,
        staleAsNull: Boolean = false,
        allowNeedsMigration: Boolean = false,
        content: kotlinx.serialization.json.JsonObjectBuilder.() -> Unit,
    ): JsonObject {
        val keys = GuideKeys(lease.snapshot.key)
        val result = command(operation) {
            put("leaseKey", lease.leaseKey)
            put("ownerId", ownerId)
            put("nowMs", nowMillis())
            put("generationKey", keys.generationKey(lease.snapshot.generation))
            content()
        }.objectResult()
        val status = result["status"]?.jsonPrimitive?.content
        if (allowNeedsMigration && status == "needsMigration") return result
        if (!staleAsNull || status != "stale") result.throwExpectedFailure()
        return result
    }

    private suspend fun command(
        operation: String,
        content: kotlinx.serialization.json.JsonObjectBuilder.() -> Unit,
    ): JsonElement {
        val operationId = "air-guide-$ownerId-${++operationCounter}"
        val command = buildJsonObject {
            put("op", operation)
            put("operationId", operationId)
            content()
        }
        return try {
            guideJson.parseToJsonElement(
                executor.execute(databaseName, operationId, guideJson.encodeToString(JsonObject.serializer(), command)),
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: BrowserIndexedDbFailure) {
            throw CatalogStoreException("Browser guide storage $operation failed (${failure.code})", failure)
        } catch (failure: Throwable) {
            throw CatalogStoreException("Browser guide storage operation failed", failure)
        }
    }

    private fun DurableGuideGeneration.ownedGeneration(): Generation? =
        (this as? Generation)?.takeIf { it.ownerId == ownerId }

    private fun DurableGuideSnapshotLease.ownedLease(): Lease =
        (this as? Lease)?.takeIf { it.ownerId == ownerId } ?: throw DurableGuideStoreException.Stale()

    private fun DurableGuideCursor?.ownedCursor(domain: String): Cursor? {
        if (this == null) return null
        return (this as? Cursor)?.takeIf { it.ownerId == ownerId && it.domain == domain }
            ?: throw DurableGuideStoreException.Stale()
    }

    private data class Generation(
        val ownerId: String,
        override val key: DurableGuideKey,
        val value: Long,
        val mutationEpoch: Long,
        val retention: DurableGuideRetention,
    ) : DurableGuideGeneration {
        override fun toString(): String = "DurableGuideGeneration(<redacted>)"
    }

    internal data class Snapshot(
        val ownerId: String,
        override val key: DurableGuideKey,
        override val generation: Long,
        override val revision: Long,
        override val mutationEpoch: Long,
        override val counts: DurableGuideCounts,
        override val retention: DurableGuideRetention,
    ) : DurableGuideSnapshot {
        override fun toString(): String = "DurableGuideSnapshot(<redacted>, revision=$revision)"
    }

    private data class Lease(val ownerId: String, val leaseKey: String, val snapshot: Snapshot) : DurableGuideSnapshotLease {
        override fun toString(): String = "DurableGuideSnapshotLease(<redacted>)"
    }

    private data class Cursor(val ownerId: String, val domain: String, val afterKey: String) : DurableGuideCursor {
        override fun toString(): String = "DurableGuideCursor(<redacted>)"
    }

    private data class Locator(val ownerId: String, val generationKey: String, val recordKey: String) : DurableGuideProgrammeLocator {
        override fun toString(): String = "DurableGuideProgrammeLocator(<redacted>)"
    }

    private data class SourceToken(
        val ownerId: String,
        val sourceKey: DurableGuideSourceKey,
        val epoch: Long,
        val mutation: Long,
    ) : DurableGuideSourceToken {
        override fun toString(): String = "DurableGuideSourceToken(<redacted>)"
    }
}

private data class GuideKeys(val key: DurableGuideKey) {
    val source = key.sourceKey.value
    val feed = key.feedId.value.encodeComponent()
    val sourceStateKey = sourceStateKey(key.sourceKey)
    val feedStateKey = "GF|$source|$feed"
    val generationPrefix = "GG|$source|$feed|"
    val activeFeedBase = "GA|$source|"
    val channelBase = "GC|$source|$feed|"
    val programmeBase = "GP|$source|$feed|"
    val finiteStartBase = "GW|$source|$feed|"
    val openStartBase = "GO|$source|$feed|"

    fun generationKey(generation: Long): String = generationPrefix + positiveKey(generation)
    fun channelPrefix(generation: String): String = "$channelBase$generation|"
    fun programmePrefix(generation: String): String = "$programmeBase$generation|"
}

private fun sourceStateKey(source: DurableGuideSourceKey): String = "GS|${source.value}"
private fun activeFeedPrefix(source: DurableGuideSourceKey, epoch: Long): String =
    "GA|${source.value}|${positiveKey(epoch)}|"

private fun kotlinx.serialization.json.JsonObjectBuilder.putKeys(keys: GuideKeys) {
    put("sourceKey", keys.source)
    put("feedId", keys.key.feedId.value)
    put("sourceStateKey", keys.sourceStateKey)
    put("feedStateKey", keys.feedStateKey)
    put("generationPrefix", keys.generationPrefix)
    put("activeFeedBase", keys.activeFeedBase)
    put("feedComponent", keys.feed)
    put("channelBase", keys.channelBase)
    put("programmeBase", keys.programmeBase)
    put("finiteStartBase", keys.finiteStartBase)
    put("openStartBase", keys.openStartBase)
}

private fun DurableGuideRetention.toJson(): JsonObject = buildJsonObject {
    put("anchorMs", anchor.toEpochMilliseconds())
    put("retainedFromMs", retainedFrom.toEpochMilliseconds())
    put("retainedUntilMs", retainedUntil.toEpochMilliseconds())
}

private fun DurableGuideCounts.toJson(): JsonObject = buildJsonObject {
    put("channels", channels)
    put("programmes", programmes)
}

private fun DurableGuideChannelRecord.toJson(prefix: String): JsonObject = buildJsonObject {
    put("key", prefix + key.value)
    put("channelKey", key.value)
    put("winnerKey", winnerKey.value)
    put("displayNames", buildJsonArray { displayNames.forEach { add(JsonPrimitive(it)) } })
    put("artworkReference", artworkReference)
}

private fun DurableGuideProgrammeRecord.toJson(prefix: String): JsonObject {
    val startMs = start.toEpochMilliseconds()
    val effectiveEndMs = effectiveEnd.toEpochMilliseconds()
    val recordKey = prefix + channelKey.value + "|" + signedKey(startMs)
    val endKey = "GE|" + prefix.removePrefix("GP|") + channelKey.value + "|" +
        signedKey(effectiveEndMs) + "|" + signedKey(startMs)
    return buildJsonObject {
        put("key", recordKey)
        put("endKey", endKey)
        put("channelKey", channelKey.value)
        put("winnerKey", winnerKey.value)
        put("startMs", startMs)
        put("endMs", end?.toEpochMilliseconds())
        put("effectiveEndMs", effectiveEndMs)
        put("title", title)
        put("subtitle", subtitle)
        put("description", description)
        put("categories", buildJsonArray { categories.forEach { add(JsonPrimitive(it)) } })
        put("episode", episode)
        put("artworkReference", artworkReference)
    }
}

private fun JsonObject.toChannel(): DurableGuideChannelRecord = DurableGuideChannelRecord(
    key = DurableGuideChannelKey(requiredString("channelKey")),
    displayNames = requiredArray("displayNames").map { it.jsonPrimitive.content },
    artworkReference = optionalString("artworkReference"),
)

private fun JsonObject.toProgramme(): DurableGuideProgrammeRecord = DurableGuideProgrammeRecord(
    channelKey = DurableGuideChannelKey(requiredString("channelKey")),
    start = requiredInstant("startMs"),
    end = optionalLong("endMs")?.let(Instant::fromEpochMilliseconds),
    title = requiredString("title"),
    subtitle = optionalString("subtitle"),
    description = optionalString("description"),
    categories = requiredArray("categories").map { it.jsonPrimitive.content },
    episode = optionalString("episode"),
    artworkReference = optionalString("artworkReference"),
)

private fun JsonObject.toSnapshot(ownerId: String): DurableGuideSnapshot {
    val source = DurableGuideSourceKey(requiredString("sourceKey"))
    val key = DurableGuideKey(source, DurableGuideFeedId(requiredString("feedId")))
    val counts = requiredElement("counts").jsonObject
    val retention = requiredElement("retention").jsonObject
    return IndexedDbDurableGuideStore.Snapshot(
        ownerId,
        key,
        requiredLong("generation"),
        requiredLong("revision"),
        requiredLong("mutationEpoch"),
        DurableGuideCounts(counts.requiredLong("channels"), counts.requiredLong("programmes")),
        DurableGuideRetention(
            Instant.fromEpochMilliseconds(retention.requiredLong("anchorMs")),
            Instant.fromEpochMilliseconds(retention.requiredLong("retainedFromMs")),
            Instant.fromEpochMilliseconds(retention.requiredLong("retainedUntilMs")),
        ),
    )
}

private fun JsonObject.optionalSnapshot(name: String, ownerId: String): DurableGuideSnapshot? =
    this[name]?.takeUnless { it is JsonNull }?.jsonObject?.toSnapshot(ownerId)

private fun JsonObject.optionalProgramme(name: String): DurableGuideProgrammeRecord? =
    this[name]?.takeUnless { it is JsonNull }?.jsonObject?.toProgramme()

private fun JsonObject.throwExpectedFailure() {
    when (requiredString("status")) {
        "ok", "terminal" -> Unit
        "stale" -> throw DurableGuideStoreException.Stale()
        "limit" -> throw DurableGuideStoreException.Limit()
        "corrupt" -> throw DurableGuideStoreException.Corrupt()
        else -> throw DurableGuideStoreException.Corrupt()
    }
}

private fun JsonElement.objectResult(): JsonObject = jsonObject
private fun JsonObject.requireStatus(expected: String) {
    if (requiredString("status") != expected) throw DurableGuideStoreException.Corrupt()
}
private fun JsonObject.requiredElement(name: String): JsonElement =
    this[name] ?: throw DurableGuideStoreException.Corrupt()
private fun JsonObject.requiredArray(name: String): JsonArray =
    this[name]?.jsonArray ?: throw DurableGuideStoreException.Corrupt()
private fun JsonObject.requiredString(name: String): String =
    this[name]?.jsonPrimitive?.contentOrNull ?: throw DurableGuideStoreException.Corrupt()
private fun JsonObject.optionalString(name: String): String? =
    this[name]?.takeUnless { it is JsonNull }?.jsonPrimitive?.contentOrNull
private fun JsonObject.requiredLong(name: String): Long =
    this[name]?.jsonPrimitive?.longOrNull ?: throw DurableGuideStoreException.Corrupt()
private fun JsonObject.optionalLong(name: String): Long? =
    this[name]?.takeUnless { it is JsonNull }?.jsonPrimitive?.longOrNull
private fun JsonObject.requiredBoolean(name: String): Boolean = when (this[name]?.jsonPrimitive?.content) {
    "true" -> true
    "false" -> false
    else -> throw DurableGuideStoreException.Corrupt()
}
private fun JsonObject.requiredInstant(name: String): Instant = Instant.fromEpochMilliseconds(requiredLong(name))

private fun kotlinx.serialization.json.JsonObjectBuilder.put(name: String, value: String?) {
    put(name, value?.let(::JsonPrimitive) ?: JsonNull)
}
private fun kotlinx.serialization.json.JsonObjectBuilder.put(name: String, value: Long?) {
    put(name, value?.let(::JsonPrimitive) ?: JsonNull)
}

private fun String.encodeComponent(): String = buildString(length * 2) {
    encodeToByteArray().forEach { byte ->
        val value = byte.toInt() and 0xff
        append(HEX[value ushr 4])
        append(HEX[value and 0x0f])
    }
}

private fun positiveKey(value: Long): String {
    require(value in 0..9_007_199_254_740_991L)
    return value.toString().padStart(16, '0')
}

private fun signedKey(value: Long): String =
    (value xor Long.MIN_VALUE).toULong().toString(16).padStart(16, '0')

private fun randomOpaque(prefix: String): String =
    prefix + "-" + Random.nextLong().toULong().toString(16) + Random.nextLong().toULong().toString(16)

private val guideJson = Json { explicitNulls = true; ignoreUnknownKeys = false }
private const val HEX = "0123456789abcdef"
