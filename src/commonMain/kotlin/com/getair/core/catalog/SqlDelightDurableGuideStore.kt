package com.getair.core.catalog

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlPreparedStatement
import com.getair.core.catalog.db.AirCatalogDatabase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.datetime.Instant
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.math.min

internal fun interface GuideReadProbe {
    fun afterSourceTokenValidated()

    companion object {
        val None = GuideReadProbe { }
    }
}

/**
 * SQLite implementation of the feed-scoped durable guide contract.
 *
 * The parent catalog store owns [driver]. This capability shares its single-writer
 * mutex and dispatcher and therefore never closes the connection itself.
 */
internal class SqlDelightDurableGuideStore(
    private val driver: SqlDriver,
    private val database: AirCatalogDatabase,
    private val dispatcher: CoroutineDispatcher,
    private val writer: Mutex,
    private val nowMillis: () -> Long,
    private val readProbe: GuideReadProbe = GuideReadProbe.None,
) : DurableGuideStore {
    override val leaseIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_LEASE_IDLE_TIMEOUT_MILLIS
    override val generationIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_GENERATION_IDLE_TIMEOUT_MILLIS

    private val owner = Any()
    private val ownerId = "${nowMillis().toString(16)}-${owner.hashCode().toUInt().toString(16)}"

    internal var lastCleanupWorkForTest: GuideCleanupWork = GuideCleanupWork()
        private set

    override suspend fun beginRefresh(
        key: DurableGuideKey,
        retention: DurableGuideRetention,
    ): DurableGuideGeneration = write {
        val now = nowMillis()
        var generation = 0L
        var mutation = 0L
        database.transaction {
            ensureSourceAndGuide(key)
            val source = requireNotNull(sourceState(key.sourceKey))
            var state = requireNotNull(state(key))
            if (state.sourceEpoch != source.deleteEpoch) {
                retireGuideState(key, state, source.deleteEpoch, now)
                state = requireNotNull(state(key))
            }
            require(state.nextGeneration in 1 until Long.MAX_VALUE) { "Guide generation space is exhausted" }
            state.latestGeneration?.let { supersedeGeneration(key, it, now) }
            val stagedOnlyDelta = if (state.activeGeneration == null && state.latestGeneration == null) 1L else 0L
            generation = state.nextGeneration
            mutation = checkedIncrement(state.mutationEpoch)
            execute(
                """
                UPDATE guide_state
                SET next_generation = ?, latest_generation = ?, mutation_epoch = ?,
                    source_epoch = ?, deleted = 0
                WHERE source_key = ? AND feed_id = ?
                """.trimIndent(),
                generation + 1,
                generation,
                mutation,
                source.deleteEpoch,
                key.sourceKey.value,
                key.feedId.value,
            )
            execute(
                """
                INSERT INTO guide_generation(
                  source_key, feed_id, generation, status, writer_epoch,
                  created_at_ms, expires_at_ms, retention_anchor_ms,
                  retained_from_ms, retained_until_ms
                ) VALUES (?, ?, ?, 'staging', ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                key.sourceKey.value,
                key.feedId.value,
                generation,
                mutation,
                now,
                checkedAdd(now, generationIdleTimeoutMillis),
                retention.anchor.toEpochMilliseconds(),
                retention.retainedFrom.toEpochMilliseconds(),
                retention.retainedUntil.toEpochMilliseconds(),
            )
            execute("UPDATE guide_source_state SET deleted = 0 WHERE source_key = ?", key.sourceKey.value)
            adjustSourceCounts(key.sourceKey, stagedOnlyDelta = stagedOnlyDelta)
            bumpSource(key.sourceKey)
        }
        SqlGuideGeneration(owner, key, generation, mutation)
    }

    override suspend fun renewGeneration(generation: DurableGuideGeneration): Boolean = write {
        val token = ownedGeneration(generation)
        val now = nowMillis()
        val row = generation(token.key, token.generation) ?: return@write false
        val current = state(token.key) ?: return@write false
        if (
            row.status != STATUS_STAGING || row.writerEpoch != token.mutationEpoch ||
            row.expiresAtMillis <= now || current.latestGeneration != token.generation ||
            !isCurrentSourceEpoch(token.key, current)
        ) return@write false
        execute(
            """
            UPDATE guide_generation SET expires_at_ms = ?
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND status = 'staging'
            """.trimIndent(),
            checkedAdd(now, generationIdleTimeoutMillis),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
        )
        true
    }

    override suspend fun abandon(generation: DurableGuideGeneration): Boolean = write {
        val token = ownedGeneration(generation)
        var changed = false
        database.transaction {
            val row = generation(token.key, token.generation) ?: return@transaction
            if (row.status != STATUS_STAGING && row.status != STATUS_FAILED) return@transaction
            val state = state(token.key) ?: return@transaction
            val stagedOnly = state.latestGeneration == token.generation && state.activeGeneration == null &&
                isCurrentSourceEpoch(token.key, state)
            execute(
                """
                UPDATE guide_generation SET status = 'abandoned'
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                """.trimIndent(),
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
            )
            enqueueCleanup(token.key, token.generation, nowMillis())
            clearLatest(token.key, token.generation)
            if (stagedOnly) adjustSourceCounts(token.key.sourceKey, stagedOnlyDelta = -1)
            bumpGuide(token.key)
            bumpSource(token.key.sourceKey)
            changed = true
        }
        changed
    }

    override suspend fun stage(
        generation: DurableGuideGeneration,
        channels: List<DurableGuideChannelRecord>,
        programmes: List<DurableGuideProgrammeRecord>,
    ): DurableGuideCounts = write {
        val token = ownedGeneration(generation)
        var result: DurableGuideCounts? = null
        var exceeded = false
        database.transaction {
            val row = generation(token.key, token.generation)
                ?: throw DurableGuideStoreException.Stale()
            val current = state(token.key) ?: throw DurableGuideStoreException.Stale()
            when (row.status) {
                STATUS_FAILED -> throw DurableGuideStoreException.Limit()
                STATUS_STAGING -> Unit
                else -> throw DurableGuideStoreException.Stale()
            }
            val now = nowMillis()
            if (
                row.writerEpoch != token.mutationEpoch || row.expiresAtMillis <= now ||
                current.latestGeneration != token.generation || !isCurrentSourceEpoch(token.key, current)
            ) throw DurableGuideStoreException.Stale()

            val batches = checkedIncrement(row.attemptedBatches)
            val inputChannels = checkedAdd(row.attemptedChannelRows, channels.size.toLong())
            val inputProgrammes = checkedAdd(row.attemptedProgrammeRows, programmes.size.toLong())
            execute(
                """
                UPDATE guide_generation
                SET attempted_batches = ?, attempted_channel_rows = ?, attempted_programme_rows = ?
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                """.trimIndent(),
                batches,
                inputChannels,
                inputProgrammes,
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
            )
            if (
                channels.size + programmes.size > DurableGuideLimits.MAX_BATCH_ITEMS ||
                batches > DurableGuideLimits.MAX_GENERATION_BATCHES ||
                inputChannels > DurableGuideLimits.MAX_INPUT_CHANNEL_ROWS ||
                inputProgrammes > DurableGuideLimits.MAX_INPUT_PROGRAMME_ROWS
            ) {
                failGeneration(token.key, token.generation, now)
                exceeded = true
                return@transaction
            }

            val channelCandidates = linkedMapOf<DurableGuideChannelKey, DurableGuideChannelRecord>()
            channels.forEach { candidate ->
                val existing = channelCandidates[candidate.key]
                if (existing == null || DurableGuideWinnerPolicy.compareChannels(candidate, existing) < 0) {
                    channelCandidates[candidate.key] = candidate
                }
            }
            val programmeCandidates = linkedMapOf<ProgrammeIdentity, DurableGuideProgrammeRecord>()
            programmes.forEach { candidate ->
                require(
                    candidate.start < row.retention.retainedUntil &&
                        candidate.effectiveEnd > row.retention.retainedFrom,
                ) { "Programme is outside generation retention" }
                val identity = ProgrammeIdentity(candidate.channelKey, candidate.start)
                val existing = programmeCandidates[identity]
                if (existing == null || DurableGuideWinnerPolicy.compareProgrammes(candidate, existing) < 0) {
                    programmeCandidates[identity] = candidate
                }
            }

            var newChannels = 0L
            val channelWrites = ArrayList<DurableGuideChannelRecord>(channelCandidates.size)
            channelCandidates.values.forEach { candidate ->
                val existing = channel(token.key, token.generation, candidate.key)
                when {
                    existing == null -> {
                        newChannels++
                        channelWrites += candidate
                    }
                    DurableGuideWinnerPolicy.compareChannels(candidate, existing) < 0 -> channelWrites += candidate
                }
            }
            var newProgrammes = 0L
            val programmeWrites = ArrayList<Pair<Long, DurableGuideProgrammeRecord>>(programmeCandidates.size)
            var nextOrdinal = row.nextProgrammeOrdinal
            programmeCandidates.values.forEach { candidate ->
                val existing = programmeByIdentity(token.key, token.generation, candidate.channelKey, candidate.start)
                when {
                    existing == null -> {
                        newProgrammes++
                        programmeWrites += nextOrdinal++ to candidate
                    }
                    DurableGuideWinnerPolicy.compareProgrammes(candidate, existing.record) < 0 -> {
                        programmeWrites += existing.ordinal to candidate
                    }
                }
            }
            val channelCount = checkedAdd(row.channelCount, newChannels)
            val programmeCount = checkedAdd(row.programmeCount, newProgrammes)
            if (
                channelCount > DurableGuideLimits.MAX_GENERATION_CHANNELS ||
                programmeCount > DurableGuideLimits.MAX_GENERATION_PROGRAMMES
            ) {
                failGeneration(token.key, token.generation, now)
                exceeded = true
                return@transaction
            }

            channelWrites.forEach { putChannel(token.key, token.generation, it) }
            programmeWrites.forEach { (ordinal, record) ->
                putProgramme(token.key, token.generation, ordinal, record)
            }
            execute(
                """
                UPDATE guide_generation
                SET channel_count = ?, programme_count = ?, next_programme_ordinal = ?, expires_at_ms = ?
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                """.trimIndent(),
                channelCount,
                programmeCount,
                nextOrdinal,
                checkedAdd(now, generationIdleTimeoutMillis),
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
            )
            result = DurableGuideCounts(channelCount, programmeCount)
        }
        if (exceeded) throw DurableGuideStoreException.Limit()
        checkNotNull(result)
    }

    override suspend fun activate(
        generation: DurableGuideGeneration,
        expected: DurableGuideCounts,
    ): DurableGuideActivation = write {
        val token = ownedGeneration(generation)
        var result: DurableGuideActivation? = null
        database.transaction {
            val row = generation(token.key, token.generation)
            if (row?.status == STATUS_FAILED) throw DurableGuideStoreException.Limit()
            val current = state(token.key)
            if (
                row == null || current == null || row.status != STATUS_STAGING ||
                current.latestGeneration != token.generation || row.writerEpoch != token.mutationEpoch ||
                !isCurrentSourceEpoch(token.key, current)
            ) {
                result = DurableGuideActivation.Superseded(activeSnapshot(token.key))
                return@transaction
            }
            if (row.expiresAtMillis <= nowMillis()) throw DurableGuideStoreException.Stale()
            if (expected.channels == 0L && expected.programmes == 0L) {
                throw DurableGuideStoreException.Limit()
            }
            require(row.channelCount == expected.channels && row.programmeCount == expected.programmes) {
                "Staged guide counts do not match"
            }
            current.activeGeneration?.takeIf { it != token.generation }?.let {
                enqueueCleanup(token.key, it, nowMillis())
            }
            val revision = checkedIncrement(current.revision)
            execute(
                """
                UPDATE guide_generation SET status = 'published'
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                """.trimIndent(),
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
            )
            execute(
                """
                UPDATE guide_state
                SET active_generation = ?, latest_generation = NULL, revision = ?, deleted = 0
                WHERE source_key = ? AND feed_id = ?
                """.trimIndent(),
                token.generation,
                revision,
                token.key.sourceKey.value,
                token.key.feedId.value,
            )
            adjustSourceCounts(
                token.key.sourceKey,
                activeDelta = if (current.activeGeneration == null) 1 else 0,
                stagedOnlyDelta = if (current.activeGeneration == null) -1 else 0,
            )
            bumpSource(token.key.sourceKey)
            result = DurableGuideActivation.Published(
                snapshot(token.key, token.generation, revision, current.mutationEpoch, row),
            )
        }
        checkNotNull(result)
    }

    override suspend fun snapshot(key: DurableGuideKey): DurableGuideSnapshot? = read {
        var result: DurableGuideSnapshot? = null
        database.transaction { result = activeSnapshot(key) }
        result
    }

    override suspend fun snapshots(
        source: DurableGuideSourceSnapshot,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideSnapshotPage = read {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = source.token as? SqlGuideSourceToken
            ?: throw DurableGuideStoreException.Stale()
        if (token.owner !== owner || token.sourceKey != source.sourceKey) {
            throw DurableGuideStoreException.Stale()
        }
        var result: DurableGuideSnapshotPage? = null
        database.transaction {
            val currentSource = sourceState(source.sourceKey)
            if (currentSource?.mutationEpoch != token.mutationEpoch) {
                throw DurableGuideStoreException.Stale()
            }
            readProbe.afterSourceTokenValidated()
            val domain = SourceCursorDomain(source.sourceKey, token.mutationEpoch)
            val continuation = cursor(after, domain)
            val rows = query(
                """
                SELECT s.feed_id, s.active_generation, s.revision, s.mutation_epoch,
                       g.writer_epoch, g.retention_anchor_ms, g.retained_from_ms,
                       g.retained_until_ms, g.channel_count, g.programme_count
                FROM guide_state s
                JOIN guide_source_state src ON src.source_key = s.source_key
                JOIN guide_generation g
                  ON g.source_key = s.source_key AND g.feed_id = s.feed_id
                 AND g.generation = s.active_generation
                WHERE s.source_key = ? AND src.deleted = 0
                  AND s.source_epoch = src.delete_epoch AND s.deleted = 0 AND s.feed_id > ?
                ORDER BY s.feed_id
                LIMIT ?
                """.trimIndent(),
                source.sourceKey.value,
                continuation?.text ?: "",
                (limit + 1).toLong(),
            ) { row -> snapshotFromSourceRow(source.sourceKey, row) }
            val page = rows.take(limit)
            result = DurableGuideSnapshotPage(
                page,
                page.lastOrNull()?.key?.feedId?.value
                    ?.takeIf { rows.size > limit }
                    ?.let { SqlGuideCursor(owner, domain, it, null) },
            )
        }
        checkNotNull(result)
    }

    override suspend fun acquire(snapshot: DurableGuideSnapshot): DurableGuideSnapshotLease? = write {
        val token = snapshot as? SqlGuideSnapshot ?: return@write null
        if (token.owner !== owner) return@write null
        val now = nowMillis()
        var result: SqlGuideLease? = null
        database.transaction {
            // The bounded DELETE takes SQLite's writer reservation before the
            // global count, making count-and-insert atomic across store owners.
            execute(
                """
                DELETE FROM guide_lease WHERE lease_id IN (
                  SELECT lease_id FROM guide_lease
                  WHERE expires_at_ms <= ? ORDER BY expires_at_ms, lease_id LIMIT 1
                )
                """.trimIndent(),
                now,
            )
            val exists = queryLong(
                """
                SELECT EXISTS(
                  SELECT 1 FROM guide_generation
                  WHERE source_key = ? AND feed_id = ? AND generation = ? AND cleanup_started = 0
                )
                """.trimIndent(),
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
            ) == 1L
            if (!exists) return@transaction
            val live = queryLong("SELECT COUNT(*) FROM guide_lease WHERE expires_at_ms > ?", now)
            if (live >= DurableGuideLimits.MAX_LIVE_LEASES) throw DurableGuideStoreException.Limit()
            execute(
                """
                INSERT INTO guide_lease(owner_id, source_key, feed_id, generation, revision, expires_at_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                ownerId,
                token.key.sourceKey.value,
                token.key.feedId.value,
                token.generation,
                token.revision,
                checkedAdd(now, leaseIdleTimeoutMillis),
            )
            val leaseId = queryLong(
                "SELECT lease_id FROM guide_lease WHERE owner_id = ? ORDER BY lease_id DESC LIMIT 1",
                ownerId,
            )
            result = SqlGuideLease(owner, leaseId, token.key, token.generation, token.revision)
        }
        result
    }

    override suspend fun renew(lease: DurableGuideSnapshotLease): Boolean = write {
        val token = lease as? SqlGuideLease ?: return@write false
        if (token.owner !== owner || !token.valid) return@write false
        val now = nowMillis()
        val live = queryLong(
            """
            SELECT EXISTS(
              SELECT 1 FROM guide_lease
              WHERE lease_id = ? AND owner_id = ? AND expires_at_ms > ?
            )
            """.trimIndent(),
            token.leaseId,
            ownerId,
            now,
        ) == 1L
        if (!live) {
            token.valid = false
            execute("DELETE FROM guide_lease WHERE lease_id = ? AND owner_id = ?", token.leaseId, ownerId)
            return@write false
        }
        execute(
            "UPDATE guide_lease SET expires_at_ms = ? WHERE lease_id = ? AND owner_id = ?",
            checkedAdd(now, leaseIdleTimeoutMillis),
            token.leaseId,
            ownerId,
        )
        true
    }

    override suspend fun release(lease: DurableGuideSnapshotLease) {
        write {
            val token = lease as? SqlGuideLease ?: return@write
            if (token.owner !== owner) return@write
            token.valid = false
            execute("DELETE FROM guide_lease WHERE lease_id = ? AND owner_id = ?", token.leaseId, ownerId)
        }
    }

    override suspend fun channels(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideChannelPage = read {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = validLease(lease)
        val domain = ChannelCursorDomain(token.leaseId)
        val continuation = cursor(after, domain)?.text ?: ""
        val rows = query(
            """
            SELECT channel_key, winner_key, display_names, artwork_reference
            FROM guide_channel
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key > ?
            ORDER BY channel_key
            LIMIT ?
            """.trimIndent(),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
            continuation,
            (limit + 1).toLong(),
        ) { decodeChannel(it) }
        val page = rows.take(limit)
        DurableGuideChannelPage(
            page,
            page.lastOrNull()?.key?.value
                ?.takeIf { rows.size > limit }
                ?.let { SqlGuideCursor(owner, domain, it, null) },
        )
    }

    override suspend fun programmeSearchRows(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideProgrammeSearchPage = read {
        require(limit in 1..DurableGuideLimits.MAX_PAGE_ITEMS)
        val token = validLease(lease)
        val domain = SearchCursorDomain(token.leaseId)
        val continuation = cursor(after, domain)
        val rows = query(
            """
            SELECT ordinal, channel_key, start_ms, effective_end_ms, title, subtitle
            FROM guide_programme
            WHERE source_key = ? AND feed_id = ? AND generation = ?
              AND (channel_key > ? OR (channel_key = ? AND start_ms > ?))
            ORDER BY channel_key, start_ms, ordinal
            LIMIT ?
            """.trimIndent(),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
            continuation?.text ?: "",
            continuation?.text ?: "",
            continuation?.number ?: Long.MIN_VALUE,
            (limit + 1).toLong(),
        ) { row ->
            SearchResultRow(
                channelKey = requiredString(row, 1),
                startMillis = requiredLong(row, 2),
                value = DurableGuideProgrammeSearchRow(
                    locator = SqlGuideLocator(
                        owner,
                        token.key,
                        token.generation,
                        token.revision,
                        requiredLong(row, 0),
                    ),
                    start = Instant.fromEpochMilliseconds(requiredLong(row, 2)),
                    effectiveEnd = Instant.fromEpochMilliseconds(requiredLong(row, 3)),
                    title = requiredString(row, 4),
                    subtitle = row.getString(5),
                ),
            )
        }
        val page = rows.take(limit)
        DurableGuideProgrammeSearchPage(
            page.map(SearchResultRow::value),
            page.lastOrNull()
                ?.takeIf { rows.size > limit }
                ?.let { SqlGuideCursor(owner, domain, it.channelKey, it.startMillis) },
        )
    }

    override suspend fun programme(
        lease: DurableGuideSnapshotLease,
        locator: DurableGuideProgrammeLocator,
    ): DurableGuideProgrammeRecord? = read {
        val token = lease as? SqlGuideLease ?: return@read null
        val location = locator as? SqlGuideLocator ?: return@read null
        if (
            token.owner !== owner || location.owner !== owner || !token.valid ||
            token.key != location.key || token.generation != location.generation ||
            token.revision != location.revision || !isLeaseLive(token)
        ) return@read null
        queryOne(
            """
            SELECT ordinal, channel_key, winner_key, start_ms, end_ms, effective_end_ms,
                   title, subtitle, description, categories, episode, artwork_reference
            FROM guide_programme
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND ordinal = ?
            """.trimIndent(),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
            location.ordinal,
        ) { decodeProgramme(it).record }
    }

    override suspend fun window(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        after: DurableGuideCursor?,
        limit: Int,
    ): DurableGuideWindowPage = read {
        require(from < until)
        require(limit in 1..DurableGuideLimits.MAX_WINDOW_ITEMS)
        val token = validLease(lease)
        val domain = WindowCursorDomain(token.leaseId, channelKey, from, until)
        val continuation = cursor(after, domain)?.number ?: Long.MIN_VALUE
        val rows = queryProgrammeWindow(token, channelKey, from, until, continuation, limit + 1)
        val page = rows.take(limit)
        val more = rows.size > limit
        DurableGuideWindowPage(
            page.map(ProgrammeRow::record),
            page.lastOrNull()
                ?.takeIf { more }
                ?.let { SqlGuideCursor(owner, domain, null, it.record.start.toEpochMilliseconds()) },
            more,
        )
    }

    override suspend fun nowNext(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        at: Instant,
    ): DurableGuideNowNext = read {
        val token = validLease(lease)
        val atMillis = at.toEpochMilliseconds()
        val current = queryOne(
            """
            SELECT ordinal, channel_key, winner_key, start_ms, end_ms, effective_end_ms,
                   title, subtitle, description, categories, episode, artwork_reference
            FROM guide_programme
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
              AND start_ms <= ? AND effective_end_ms > ?
            ORDER BY start_ms DESC, ordinal
            LIMIT 1
            """.trimIndent(),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
            channelKey.value,
            atMillis,
            atMillis,
        ) { decodeProgramme(it).record }
        val next = queryOne(
            """
            SELECT ordinal, channel_key, winner_key, start_ms, end_ms, effective_end_ms,
                   title, subtitle, description, categories, episode, artwork_reference
            FROM guide_programme
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
              AND start_ms > ?
            ORDER BY start_ms, ordinal
            LIMIT 1
            """.trimIndent(),
            token.key.sourceKey.value,
            token.key.feedId.value,
            token.generation,
            channelKey.value,
            atMillis,
        ) { decodeProgramme(it).record }
        DurableGuideNowNext(current, next)
    }

    override suspend fun multiChannelWindow(
        lease: DurableGuideSnapshotLease,
        request: DurableGuideMultiChannelWindowRequest,
    ): DurableGuideMultiChannelWindow = read {
        val token = validLease(lease)
        var remaining = request.totalLimit
        var payloadBytes = 0
        var payloadExhausted = false
        var anyTruncated = false
        val windows = request.channelKeys.map { channelKey ->
            val allowed = min(request.perChannelLimit, remaining)
            val rows = if (allowed == 0 || payloadExhausted) {
                emptyList()
            } else {
                queryProgrammeWindow(token, channelKey, request.from, request.until, Long.MIN_VALUE, allowed + 1)
            }
            val accepted = ArrayList<DurableGuideProgrammeRecord>(min(allowed, rows.size))
            rows.take(allowed).forEach { row ->
                if (!payloadExhausted) {
                    val bytes = DurableGuidePayloadSizing.programmeBytes(row.record)
                    if (payloadBytes + bytes > request.payloadByteLimit) {
                        payloadExhausted = true
                    } else {
                        accepted += row.record
                        payloadBytes += bytes
                        remaining--
                    }
                }
            }
            val hasAny = if (rows.isNotEmpty()) true else programmeExists(token, channelKey, request.from, request.until)
            val truncated = (hasAny && accepted.size < rows.size) ||
                (rows.size > allowed) || (hasAny && (allowed == 0 || payloadExhausted) && accepted.isEmpty())
            anyTruncated = anyTruncated || truncated
            DurableGuideChannelWindow(channelKey, accepted, truncated)
        }
        DurableGuideMultiChannelWindow(windows, payloadBytes, anyTruncated)
    }

    override suspend fun prune(
        key: DurableGuideKey,
        expectedRevision: Long,
        expectedMutationEpoch: Long,
        retention: DurableGuideRetention,
    ): DurableGuidePruneResult = write {
        var result: DurableGuidePruneResult? = null
        database.transaction {
            val current = state(key)
            val active = activeSnapshot(key)
            if (
                current == null || active == null || current.revision != expectedRevision ||
                current.mutationEpoch != expectedMutationEpoch
            ) {
                result = DurableGuidePruneResult.Superseded(active)
                return@transaction
            }
            val activeRow = generation(key, checkNotNull(current.activeGeneration))
                ?: throw DurableGuideStoreException.Corrupt()
            if (
                retention.anchor != activeRow.retention.anchor ||
                retention.retainedFrom < activeRow.retention.retainedFrom ||
                retention.retainedUntil > activeRow.retention.retainedUntil
            ) throw DurableGuideStoreException.Limit()
            val retainedCount = queryLong(
                """
                SELECT COUNT(*) FROM guide_programme
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                  AND start_ms < ? AND effective_end_ms > ?
                """.trimIndent(),
                key.sourceKey.value,
                key.feedId.value,
                current.activeGeneration,
                retention.retainedUntil.toEpochMilliseconds(),
                retention.retainedFrom.toEpochMilliseconds(),
            )
            terminalizeWriters(key, STATUS_SUPERSEDED, nowMillis())
            val mutation = checkedIncrement(current.mutationEpoch)
            if (retainedCount == activeRow.programmeCount) {
                execute(
                    "UPDATE guide_state SET mutation_epoch = ?, latest_generation = NULL WHERE source_key = ? AND feed_id = ?",
                    mutation,
                    key.sourceKey.value,
                    key.feedId.value,
                )
                bumpSource(key.sourceKey)
                result = DurableGuidePruneResult.Unchanged(
                    snapshot(key, current.activeGeneration, current.revision, mutation, activeRow),
                )
                return@transaction
            }
            require(current.nextGeneration in 1 until Long.MAX_VALUE)
            val newGeneration = current.nextGeneration
            val revision = checkedIncrement(current.revision)
            execute(
                """
                INSERT INTO guide_generation(
                  source_key, feed_id, generation, status, writer_epoch,
                  created_at_ms, expires_at_ms, retention_anchor_ms, retained_from_ms,
                  retained_until_ms, channel_count, programme_count, next_programme_ordinal
                ) VALUES (?, ?, ?, 'published', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                key.sourceKey.value,
                key.feedId.value,
                newGeneration,
                mutation,
                nowMillis(),
                nowMillis(),
                retention.anchor.toEpochMilliseconds(),
                retention.retainedFrom.toEpochMilliseconds(),
                retention.retainedUntil.toEpochMilliseconds(),
                activeRow.channelCount,
                retainedCount,
                activeRow.nextProgrammeOrdinal,
            )
            execute(
                """
                INSERT INTO guide_channel
                SELECT source_key, feed_id, ?, channel_key, winner_key, display_names, artwork_reference
                FROM guide_channel
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                """.trimIndent(),
                newGeneration,
                key.sourceKey.value,
                key.feedId.value,
                current.activeGeneration,
            )
            execute(
                """
                INSERT INTO guide_programme
                SELECT source_key, feed_id, ?, ordinal, channel_key, winner_key, start_ms, end_ms,
                       effective_end_ms, title, subtitle, description, categories, episode, artwork_reference
                FROM guide_programme
                WHERE source_key = ? AND feed_id = ? AND generation = ?
                  AND start_ms < ? AND effective_end_ms > ?
                """.trimIndent(),
                newGeneration,
                key.sourceKey.value,
                key.feedId.value,
                current.activeGeneration,
                retention.retainedUntil.toEpochMilliseconds(),
                retention.retainedFrom.toEpochMilliseconds(),
            )
            enqueueCleanup(key, current.activeGeneration, nowMillis())
            execute(
                """
                UPDATE guide_state
                SET active_generation = ?, latest_generation = NULL, next_generation = ?,
                    revision = ?, mutation_epoch = ?, deleted = 0
                WHERE source_key = ? AND feed_id = ?
                """.trimIndent(),
                newGeneration,
                newGeneration + 1,
                revision,
                mutation,
                key.sourceKey.value,
                key.feedId.value,
            )
            bumpSource(key.sourceKey)
            val newRow = generation(key, newGeneration) ?: throw DurableGuideStoreException.Corrupt()
            result = DurableGuidePruneResult.Published(snapshot(key, newGeneration, revision, mutation, newRow))
        }
        checkNotNull(result)
    }

    override suspend fun deleteGuide(
        key: DurableGuideKey,
        expectedRevision: Long?,
        expectedMutationEpoch: Long?,
    ): DurableGuideDeleteResult = write {
        if ((expectedRevision == null) != (expectedMutationEpoch == null)) {
            throw DurableGuideStoreException.Stale()
        }
        var result: DurableGuideDeleteResult? = null
        database.transaction {
            ensureSourceAndGuide(key)
            val current = requireNotNull(state(key))
            val active = activeSnapshot(key)
            val visible = isCurrentSourceEpoch(key, current)
            if (
                expectedRevision != null &&
                (current.revision != expectedRevision || current.mutationEpoch != expectedMutationEpoch)
            ) {
                result = DurableGuideDeleteResult.Superseded(active)
                return@transaction
            }
            terminalizeWriters(key, STATUS_ABANDONED, nowMillis())
            current.activeGeneration?.let { enqueueCleanup(key, it, nowMillis()) }
            val revision = checkedIncrement(current.revision)
            val mutation = checkedIncrement(current.mutationEpoch)
            execute(
                """
                UPDATE guide_state
                SET active_generation = NULL, latest_generation = NULL, revision = ?,
                    mutation_epoch = ?, deleted = 1
                WHERE source_key = ? AND feed_id = ?
                """.trimIndent(),
                revision,
                mutation,
                key.sourceKey.value,
                key.feedId.value,
            )
            if (visible) {
                adjustSourceCounts(
                    key.sourceKey,
                    activeDelta = if (current.activeGeneration != null) -1 else 0,
                    stagedOnlyDelta = if (current.activeGeneration == null && current.latestGeneration != null) -1 else 0,
                )
            }
            bumpSource(key.sourceKey)
            result = DurableGuideDeleteResult.Deleted(revision)
        }
        checkNotNull(result)
    }

    override suspend fun sourceSnapshot(sourceKey: DurableGuideSourceKey): DurableGuideSourceSnapshot = read {
        val source = sourceState(sourceKey)
        val mutation = source?.mutationEpoch ?: 0L
        val count = if (source == null || source.deleted) 0 else source.activeFeedCount.toBoundedInt()
        DurableGuideSourceSnapshot(sourceKey, count, SqlGuideSourceToken(owner, sourceKey, mutation))
    }

    override suspend fun deleteSource(
        sourceKey: DurableGuideSourceKey,
        expected: DurableGuideSourceToken?,
    ): DurableGuideSourceDeleteResult = write {
        var result: DurableGuideSourceDeleteResult? = null
        database.transaction {
            ensureSource(sourceKey)
            val source = sourceState(sourceKey) ?: throw DurableGuideStoreException.Corrupt()
            val activeCount = if (source.deleted) 0 else source.activeFeedCount.toBoundedInt()
            val stagedOnlyCount = if (source.deleted) 0 else source.stagedOnlyFeedCount.toBoundedInt()
            if (expected != null) {
                val token = expected as? SqlGuideSourceToken
                if (
                    token == null || token.owner !== owner || token.sourceKey != sourceKey ||
                    token.mutationEpoch != source.mutationEpoch
                ) {
                    result = DurableGuideSourceDeleteResult.Superseded(activeCount, stagedOnlyCount)
                    return@transaction
                }
            }
            val now = nowMillis()
            val deleteEpoch = checkedIncrement(source.deleteEpoch)
            val mutation = checkedIncrement(source.mutationEpoch)
            execute(
                """
                UPDATE guide_source_state
                SET mutation_epoch = ?, delete_epoch = ?, active_feed_count = 0,
                    staged_only_feed_count = 0, deleted = 1
                WHERE source_key = ?
                """.trimIndent(),
                mutation,
                deleteEpoch,
                sourceKey.value,
            )
            execute(
                """
                INSERT OR REPLACE INTO guide_source_cleanup(
                  source_key, delete_epoch, after_feed_id, enqueued_at_ms
                ) VALUES (?, ?, '', ?)
                """.trimIndent(),
                sourceKey.value,
                deleteEpoch,
                now,
            )
            result = DurableGuideSourceDeleteResult.Deleted(activeCount, stagedOnlyCount)
        }
        checkNotNull(result)
    }

    override suspend fun cleanupUnreachable(maxRows: Int): DurableGuideCleanupResult = write {
        if (maxRows !in 1..DurableGuideLimits.MAX_CLEANUP_ROWS) throw DurableGuideStoreException.Limit()
        var removed = 0L
        var hasMore = false
        var expiredLeases = 0
        var retiredSourceFeeds = 0
        var expiredWriters = 0
        database.transaction {
            val now = nowMillis()
            expiredLeases = reapExpiredLeases(now, maxRows)
            if (expiredLeases == 0) {
                retiredSourceFeeds = if (retireOneSourceFeed(now)) 1 else 0
            }
            if (expiredLeases == 0 && retiredSourceFeeds == 0) {
                expiredWriters = if (expireOneWriter(now)) 1 else 0
                removed = cleanupOneGeneration(now, maxRows)
            }
            hasMore = cleanupEligible(now)
        }
        lastCleanupWorkForTest = GuideCleanupWork(
            expiredLeases = expiredLeases,
            retiredSourceFeeds = retiredSourceFeeds,
            expiredWriters = expiredWriters,
            removedPayloadRows = removed.toBoundedInt(),
        )
        DurableGuideCleanupResult(removed.toBoundedInt(), hasMore)
    }

    internal suspend fun queryPlansForTest(): List<String> = read {
        listOf(
            explain(
                """
                EXPLAIN QUERY PLAN SELECT channel_key FROM guide_channel
                WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key > ?
                ORDER BY channel_key LIMIT ?
                """.trimIndent(),
                "source", "feed", 1L, "", 10L,
            ),
            explain(
                """
                EXPLAIN QUERY PLAN SELECT ordinal FROM guide_programme
                WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
                  AND start_ms < ? AND effective_end_ms > ?
                ORDER BY start_ms, ordinal LIMIT ?
                """.trimIndent(),
                "source", "feed", 1L, "channel", Long.MAX_VALUE, Long.MIN_VALUE, 10L,
            ),
            explain(
                """
                EXPLAIN QUERY PLAN SELECT ordinal FROM guide_programme
                WHERE source_key = ? AND feed_id = ? AND generation = ? AND ordinal = ?
                """.trimIndent(),
                "source", "feed", 1L, 1L,
            ),
            explain(
                """
                EXPLAIN QUERY PLAN SELECT lease_id FROM guide_lease
                WHERE expires_at_ms <= ? ORDER BY expires_at_ms, lease_id LIMIT ?
                """.trimIndent(),
                1L, 1L,
            ),
            explain(
                """
                EXPLAIN QUERY PLAN SELECT source_key, feed_id, generation FROM guide_generation
                WHERE status = 'staging' AND expires_at_ms <= ?
                ORDER BY expires_at_ms, source_key, feed_id, generation LIMIT 1
                """.trimIndent(),
                1L,
            ),
            explain(
                """
                EXPLAIN QUERY PLAN SELECT feed_id FROM guide_state
                WHERE source_key = ? AND source_epoch < ? AND feed_id > ?
                ORDER BY feed_id LIMIT 1
                """.trimIndent(),
                "source", 1L, "",
            ),
        ).flatten()
    }

    private fun ensureSourceAndGuide(key: DurableGuideKey) {
        ensureSource(key.sourceKey)
        val source = requireNotNull(sourceState(key.sourceKey))
        execute(
            "INSERT OR IGNORE INTO guide_state(source_key, feed_id, source_epoch) VALUES (?, ?, ?)",
            key.sourceKey.value,
            key.feedId.value,
            source.deleteEpoch,
        )
    }

    private fun ensureSource(sourceKey: DurableGuideSourceKey) {
        execute("INSERT OR IGNORE INTO guide_source_state(source_key) VALUES (?)", sourceKey.value)
    }

    private fun sourceState(sourceKey: DurableGuideSourceKey): GuideSourceStateRow? = queryOne(
        """
        SELECT mutation_epoch, delete_epoch, active_feed_count, staged_only_feed_count, deleted
        FROM guide_source_state WHERE source_key = ?
        """.trimIndent(),
        sourceKey.value,
    ) { row ->
        GuideSourceStateRow(
            mutationEpoch = requiredLong(row, 0),
            deleteEpoch = requiredLong(row, 1),
            activeFeedCount = requiredLong(row, 2),
            stagedOnlyFeedCount = requiredLong(row, 3),
            deleted = requiredLong(row, 4) != 0L,
        )
    }

    private fun state(key: DurableGuideKey): GuideStateRow? = queryOne(
        """
        SELECT active_generation, latest_generation, next_generation, revision, mutation_epoch,
               source_epoch, deleted
        FROM guide_state WHERE source_key = ? AND feed_id = ?
        """.trimIndent(),
        key.sourceKey.value,
        key.feedId.value,
    ) { row ->
        GuideStateRow(
            row.getLong(0),
            row.getLong(1),
            requiredLong(row, 2),
            requiredLong(row, 3),
            requiredLong(row, 4),
            requiredLong(row, 5),
            requiredLong(row, 6) != 0L,
        )
    }

    private fun generation(key: DurableGuideKey, generation: Long): GenerationRow? = queryOne(
        """
        SELECT status, writer_epoch, expires_at_ms, retention_anchor_ms, retained_from_ms,
               retained_until_ms, attempted_batches, attempted_channel_rows,
               attempted_programme_rows, channel_count, programme_count,
               next_programme_ordinal, cleanup_started
        FROM guide_generation
        WHERE source_key = ? AND feed_id = ? AND generation = ?
        """.trimIndent(),
        key.sourceKey.value,
        key.feedId.value,
        generation,
    ) { row ->
        GenerationRow(
            status = requiredString(row, 0),
            writerEpoch = requiredLong(row, 1),
            expiresAtMillis = requiredLong(row, 2),
            retention = DurableGuideRetention(
                Instant.fromEpochMilliseconds(requiredLong(row, 3)),
                Instant.fromEpochMilliseconds(requiredLong(row, 4)),
                Instant.fromEpochMilliseconds(requiredLong(row, 5)),
            ),
            attemptedBatches = requiredLong(row, 6),
            attemptedChannelRows = requiredLong(row, 7),
            attemptedProgrammeRows = requiredLong(row, 8),
            channelCount = requiredLong(row, 9),
            programmeCount = requiredLong(row, 10),
            nextProgrammeOrdinal = requiredLong(row, 11),
            cleanupStarted = requiredLong(row, 12) != 0L,
        )
    }

    private fun activeSnapshot(key: DurableGuideKey): SqlGuideSnapshot? {
        val state = state(key) ?: return null
        val source = sourceState(key.sourceKey) ?: return null
        if (source.deleted || state.sourceEpoch != source.deleteEpoch || state.deleted) return null
        val generation = state.activeGeneration ?: return null
        val row = generation(key, generation) ?: throw DurableGuideStoreException.Corrupt()
        return snapshot(key, generation, state.revision, state.mutationEpoch, row)
    }

    private fun isCurrentSourceEpoch(key: DurableGuideKey, state: GuideStateRow): Boolean {
        val source = sourceState(key.sourceKey) ?: return false
        return !source.deleted && state.sourceEpoch == source.deleteEpoch
    }

    private fun snapshot(
        key: DurableGuideKey,
        generation: Long,
        revision: Long,
        mutationEpoch: Long,
        row: GenerationRow,
    ): SqlGuideSnapshot = SqlGuideSnapshot(
        owner,
        key,
        generation,
        revision,
        mutationEpoch,
        DurableGuideCounts(row.channelCount, row.programmeCount),
        row.retention,
    )

    private fun snapshotFromSourceRow(sourceKey: DurableGuideSourceKey, row: SqlCursor): SqlGuideSnapshot {
        val key = DurableGuideKey(sourceKey, DurableGuideFeedId(requiredString(row, 0)))
        return SqlGuideSnapshot(
            owner = owner,
            key = key,
            generation = requiredLong(row, 1),
            revision = requiredLong(row, 2),
            mutationEpoch = requiredLong(row, 3),
            counts = DurableGuideCounts(requiredLong(row, 8), requiredLong(row, 9)),
            retention = DurableGuideRetention(
                Instant.fromEpochMilliseconds(requiredLong(row, 5)),
                Instant.fromEpochMilliseconds(requiredLong(row, 6)),
                Instant.fromEpochMilliseconds(requiredLong(row, 7)),
            ),
        )
    }

    private fun ownedGeneration(value: DurableGuideGeneration): SqlGuideGeneration {
        val token = value as? SqlGuideGeneration ?: throw DurableGuideStoreException.Stale()
        if (token.owner !== owner) throw DurableGuideStoreException.Stale()
        return token
    }

    private fun validLease(value: DurableGuideSnapshotLease): SqlGuideLease {
        val token = value as? SqlGuideLease ?: throw DurableGuideStoreException.Stale()
        if (token.owner !== owner) throw DurableGuideStoreException.Stale()
        if (!token.valid || !isLeaseLive(token)) {
            token.valid = false
            throw DurableGuideStoreException.Stale()
        }
        return token
    }

    private fun isLeaseLive(token: SqlGuideLease): Boolean {
        val row = queryOne(
            """
            SELECT owner_id, source_key, feed_id, generation, revision, expires_at_ms
            FROM guide_lease WHERE lease_id = ?
            """.trimIndent(),
            token.leaseId,
        ) { cursor ->
            LeaseRow(
                requiredString(cursor, 0),
                requiredString(cursor, 1),
                requiredString(cursor, 2),
                requiredLong(cursor, 3),
                requiredLong(cursor, 4),
                requiredLong(cursor, 5),
            )
        } ?: return false
        return row.ownerId == ownerId && row.sourceKey == token.key.sourceKey.value &&
            row.feedId == token.key.feedId.value && row.generation == token.generation &&
            row.revision == token.revision && row.expiresAtMillis > nowMillis()
    }

    private fun cursor(value: DurableGuideCursor?, domain: GuideCursorDomain): SqlGuideCursor? {
        if (value == null) return null
        val token = value as? SqlGuideCursor ?: throw DurableGuideStoreException.Stale()
        if (token.owner !== owner || token.domain != domain) throw DurableGuideStoreException.Stale()
        return token
    }

    private fun channel(
        key: DurableGuideKey,
        generation: Long,
        channelKey: DurableGuideChannelKey,
    ): DurableGuideChannelRecord? = queryOne(
        """
        SELECT channel_key, winner_key, display_names, artwork_reference
        FROM guide_channel
        WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
        """.trimIndent(),
        key.sourceKey.value,
        key.feedId.value,
        generation,
        channelKey.value,
    ) { decodeChannel(it) }

    private fun putChannel(key: DurableGuideKey, generation: Long, record: DurableGuideChannelRecord) {
        execute(
            """
            INSERT OR REPLACE INTO guide_channel(
              source_key, feed_id, generation, channel_key, winner_key, display_names, artwork_reference
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
            record.key.value,
            record.winnerKey.value,
            encodeStrings(record.displayNames),
            record.artworkReference,
        )
    }

    private fun programmeByIdentity(
        key: DurableGuideKey,
        generation: Long,
        channelKey: DurableGuideChannelKey,
        start: Instant,
    ): ProgrammeRow? = queryOne(
        """
        SELECT ordinal, channel_key, winner_key, start_ms, end_ms, effective_end_ms,
               title, subtitle, description, categories, episode, artwork_reference
        FROM guide_programme
        WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ? AND start_ms = ?
        """.trimIndent(),
        key.sourceKey.value,
        key.feedId.value,
        generation,
        channelKey.value,
        start.toEpochMilliseconds(),
    ) { decodeProgramme(it) }

    private fun putProgramme(
        key: DurableGuideKey,
        generation: Long,
        ordinal: Long,
        record: DurableGuideProgrammeRecord,
    ) {
        execute(
            """
            INSERT OR REPLACE INTO guide_programme(
              source_key, feed_id, generation, ordinal, channel_key, winner_key,
              start_ms, end_ms, effective_end_ms, title, subtitle, description,
              categories, episode, artwork_reference
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
            ordinal,
            record.channelKey.value,
            record.winnerKey.value,
            record.start.toEpochMilliseconds(),
            record.end?.toEpochMilliseconds(),
            record.effectiveEnd.toEpochMilliseconds(),
            record.title,
            record.subtitle,
            record.description,
            encodeStrings(record.categories),
            record.episode,
            record.artworkReference,
        )
    }

    private fun decodeChannel(row: SqlCursor): DurableGuideChannelRecord {
        val winner = requiredString(row, 1)
        val record = DurableGuideChannelRecord(
            DurableGuideChannelKey(requiredString(row, 0)),
            decodeStrings(requiredString(row, 2)),
            row.getString(3),
        )
        if (record.winnerKey.value != winner) throw DurableGuideStoreException.Corrupt()
        return record
    }

    private fun decodeProgramme(row: SqlCursor): ProgrammeRow {
        val winner = requiredString(row, 2)
        val record = DurableGuideProgrammeRecord(
            channelKey = DurableGuideChannelKey(requiredString(row, 1)),
            start = Instant.fromEpochMilliseconds(requiredLong(row, 3)),
            end = row.getLong(4)?.let(Instant::fromEpochMilliseconds),
            title = requiredString(row, 6),
            subtitle = row.getString(7),
            description = row.getString(8),
            categories = decodeStrings(requiredString(row, 9)),
            episode = row.getString(10),
            artworkReference = row.getString(11),
        )
        if (
            record.effectiveEnd.toEpochMilliseconds() != requiredLong(row, 5) ||
            record.winnerKey.value != winner
        ) throw DurableGuideStoreException.Corrupt()
        return ProgrammeRow(requiredLong(row, 0), record)
    }

    private fun queryProgrammeWindow(
        lease: SqlGuideLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        afterStartMillis: Long,
        limit: Int,
    ): List<ProgrammeRow> = query(
        """
        SELECT ordinal, channel_key, winner_key, start_ms, end_ms, effective_end_ms,
               title, subtitle, description, categories, episode, artwork_reference
        FROM guide_programme
        WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
          AND start_ms < ? AND effective_end_ms > ? AND start_ms > ?
        ORDER BY start_ms, ordinal
        LIMIT ?
        """.trimIndent(),
        lease.key.sourceKey.value,
        lease.key.feedId.value,
        lease.generation,
        channelKey.value,
        until.toEpochMilliseconds(),
        from.toEpochMilliseconds(),
        afterStartMillis,
        limit.toLong(),
    ) { decodeProgramme(it) }

    private fun programmeExists(
        lease: SqlGuideLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
    ): Boolean = queryLong(
        """
        SELECT EXISTS(
          SELECT 1 FROM guide_programme
          WHERE source_key = ? AND feed_id = ? AND generation = ? AND channel_key = ?
            AND start_ms < ? AND effective_end_ms > ?
        )
        """.trimIndent(),
        lease.key.sourceKey.value,
        lease.key.feedId.value,
        lease.generation,
        channelKey.value,
        until.toEpochMilliseconds(),
        from.toEpochMilliseconds(),
    ) == 1L

    private fun supersedeGeneration(key: DurableGuideKey, generation: Long, now: Long) {
        execute(
            """
            UPDATE guide_generation SET status = 'superseded'
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND status = 'staging'
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
        )
        enqueueCleanup(key, generation, now)
    }

    private fun failGeneration(key: DurableGuideKey, generation: Long, now: Long) {
        val current = state(key)
        val stagedOnly = current?.latestGeneration == generation && current.activeGeneration == null &&
            isCurrentSourceEpoch(key, current)
        execute(
            """
            UPDATE guide_generation SET status = 'failed'
            WHERE source_key = ? AND feed_id = ? AND generation = ?
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
        )
        clearLatest(key, generation)
        if (stagedOnly) adjustSourceCounts(key.sourceKey, stagedOnlyDelta = -1)
        enqueueCleanup(key, generation, now)
    }

    private fun terminalizeWriters(key: DurableGuideKey, status: String, now: Long) {
        execute(
            """
            INSERT OR IGNORE INTO guide_cleanup_queue(source_key, feed_id, generation, enqueued_at_ms)
            SELECT source_key, feed_id, generation, ? FROM guide_generation
            WHERE source_key = ? AND feed_id = ? AND status = 'staging'
            """.trimIndent(),
            now,
            key.sourceKey.value,
            key.feedId.value,
        )
        execute(
            "UPDATE guide_generation SET status = ? WHERE source_key = ? AND feed_id = ? AND status = 'staging'",
            status,
            key.sourceKey.value,
            key.feedId.value,
        )
        execute(
            "UPDATE guide_state SET latest_generation = NULL WHERE source_key = ? AND feed_id = ?",
            key.sourceKey.value,
            key.feedId.value,
        )
    }

    private fun clearLatest(key: DurableGuideKey, generation: Long) {
        execute(
            """
            UPDATE guide_state SET latest_generation = NULL
            WHERE source_key = ? AND feed_id = ? AND latest_generation = ?
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
        )
    }

    private fun enqueueCleanup(key: DurableGuideKey, generation: Long, now: Long) {
        execute(
            """
            INSERT OR IGNORE INTO guide_cleanup_queue(source_key, feed_id, generation, enqueued_at_ms)
            VALUES (?, ?, ?, ?)
            """.trimIndent(),
            key.sourceKey.value,
            key.feedId.value,
            generation,
            now,
        )
    }

    private fun reapExpiredLeases(now: Long, limit: Int): Int {
        execute(
            """
            DELETE FROM guide_lease WHERE lease_id IN (
              SELECT lease_id FROM guide_lease
              WHERE expires_at_ms <= ? ORDER BY expires_at_ms, lease_id LIMIT ?
            )
            """.trimIndent(),
            now,
            limit.toLong(),
        )
        return changedRows().toBoundedInt()
    }

    private fun retireOneSourceFeed(now: Long): Boolean {
        val job = queryOne(
            """
            SELECT source_key, delete_epoch, after_feed_id
            FROM guide_source_cleanup
            ORDER BY enqueued_at_ms, source_key
            LIMIT 1
            """.trimIndent(),
        ) { row -> SourceCleanupJob(requiredString(row, 0), requiredLong(row, 1), requiredString(row, 2)) }
            ?: return false
        val row = queryOne(
            """
            SELECT feed_id, active_generation, latest_generation, next_generation,
                   revision, mutation_epoch, source_epoch, deleted
            FROM guide_state
            WHERE source_key = ? AND source_epoch < ? AND feed_id > ?
            ORDER BY feed_id
            LIMIT 1
            """.trimIndent(),
            job.sourceKey,
            job.deleteEpoch,
            job.afterFeedId,
        ) { cursor ->
            val key = DurableGuideKey(
                DurableGuideSourceKey(job.sourceKey),
                DurableGuideFeedId(requiredString(cursor, 0)),
            )
            key to GuideStateRow(
                activeGeneration = cursor.getLong(1),
                latestGeneration = cursor.getLong(2),
                nextGeneration = requiredLong(cursor, 3),
                revision = requiredLong(cursor, 4),
                mutationEpoch = requiredLong(cursor, 5),
                sourceEpoch = requiredLong(cursor, 6),
                deleted = requiredLong(cursor, 7) != 0L,
            )
        }
        if (row == null) {
            execute("DELETE FROM guide_source_cleanup WHERE source_key = ?", job.sourceKey)
            return true
        }
        retireGuideState(row.first, row.second, job.deleteEpoch, now)
        execute(
            "UPDATE guide_source_cleanup SET after_feed_id = ? WHERE source_key = ? AND delete_epoch = ?",
            row.first.feedId.value,
            job.sourceKey,
            job.deleteEpoch,
        )
        return true
    }

    private fun retireGuideState(
        key: DurableGuideKey,
        state: GuideStateRow,
        sourceEpoch: Long,
        now: Long,
    ) {
        state.activeGeneration?.let { enqueueCleanup(key, it, now) }
        state.latestGeneration?.let { generation ->
            execute(
                """
                UPDATE guide_generation SET status = 'abandoned'
                WHERE source_key = ? AND feed_id = ? AND generation = ? AND status = 'staging'
                """.trimIndent(),
                key.sourceKey.value,
                key.feedId.value,
                generation,
            )
            enqueueCleanup(key, generation, now)
        }
        execute(
            """
            UPDATE guide_state
            SET active_generation = NULL, latest_generation = NULL,
                revision = revision + 1, mutation_epoch = mutation_epoch + 1,
                source_epoch = ?, deleted = 1
            WHERE source_key = ? AND feed_id = ? AND source_epoch < ?
            """.trimIndent(),
            sourceEpoch,
            key.sourceKey.value,
            key.feedId.value,
            sourceEpoch,
        )
    }

    private fun expireOneWriter(now: Long): Boolean {
        val target = queryOne(
            """
            SELECT source_key, feed_id, generation
            FROM guide_generation
            WHERE status = 'staging' AND expires_at_ms <= ?
            ORDER BY expires_at_ms, source_key, feed_id, generation
            LIMIT 1
            """.trimIndent(),
            now,
        ) { row -> CleanupTarget(requiredString(row, 0), requiredString(row, 1), requiredLong(row, 2)) }
            ?: return false
        val key = DurableGuideKey(
            DurableGuideSourceKey(target.sourceKey),
            DurableGuideFeedId(target.feedId),
        )
        val state = state(key)
        val stagedOnly = state?.latestGeneration == target.generation && state.activeGeneration == null &&
            isCurrentSourceEpoch(key, state)
        execute(
            """
            UPDATE guide_generation SET status = 'failed'
            WHERE source_key = ? AND feed_id = ? AND generation = ? AND status = 'staging'
            """.trimIndent(),
            target.sourceKey,
            target.feedId,
            target.generation,
        )
        clearLatest(key, target.generation)
        if (stagedOnly) adjustSourceCounts(key.sourceKey, stagedOnlyDelta = -1)
        enqueueCleanup(key, target.generation, now)
        return true
    }

    private fun cleanupOneGeneration(now: Long, maxRows: Int): Long {
        val target = queryOne(
            """
            SELECT q.source_key, q.feed_id, q.generation
            FROM guide_cleanup_queue q
            JOIN guide_generation g
              ON g.source_key = q.source_key AND g.feed_id = q.feed_id AND g.generation = q.generation
            WHERE NOT EXISTS (
              SELECT 1 FROM guide_lease l
              WHERE l.source_key = q.source_key AND l.feed_id = q.feed_id
                AND l.generation = q.generation AND l.expires_at_ms > ?
            )
            ORDER BY q.enqueued_at_ms, q.source_key, q.feed_id, q.generation
            LIMIT 1
            """.trimIndent(),
            now,
        ) { row -> CleanupTarget(requiredString(row, 0), requiredString(row, 1), requiredLong(row, 2)) }
            ?: return 0L
        execute(
            """
            UPDATE guide_generation SET cleanup_started = 1
            WHERE source_key = ? AND feed_id = ? AND generation = ?
            """.trimIndent(),
            target.sourceKey,
            target.feedId,
            target.generation,
        )
        execute(
            """
            DELETE FROM guide_channel WHERE rowid IN (
              SELECT rowid FROM guide_channel
              WHERE source_key = ? AND feed_id = ? AND generation = ? LIMIT ?
            )
            """.trimIndent(),
            target.sourceKey,
            target.feedId,
            target.generation,
            maxRows.toLong(),
        )
        val channelRows = changedRows()
        val remaining = maxRows.toLong() - channelRows
        var programmeRows = 0L
        if (remaining > 0) {
            execute(
                """
                DELETE FROM guide_programme WHERE rowid IN (
                  SELECT rowid FROM guide_programme
                  WHERE source_key = ? AND feed_id = ? AND generation = ? LIMIT ?
                )
                """.trimIndent(),
                target.sourceKey,
                target.feedId,
                target.generation,
                remaining,
            )
            programmeRows = changedRows()
        }
        val hasPayload = queryLong(
            """
            SELECT EXISTS(
              SELECT 1 FROM guide_channel WHERE source_key = ? AND feed_id = ? AND generation = ?
              UNION ALL
              SELECT 1 FROM guide_programme WHERE source_key = ? AND feed_id = ? AND generation = ?
            )
            """.trimIndent(),
            target.sourceKey,
            target.feedId,
            target.generation,
            target.sourceKey,
            target.feedId,
            target.generation,
        ) == 1L
        if (!hasPayload) {
            execute(
                "DELETE FROM guide_generation WHERE source_key = ? AND feed_id = ? AND generation = ?",
                target.sourceKey,
                target.feedId,
                target.generation,
            )
            execute(
                "DELETE FROM guide_cleanup_queue WHERE source_key = ? AND feed_id = ? AND generation = ?",
                target.sourceKey,
                target.feedId,
                target.generation,
            )
        }
        return channelRows + programmeRows
    }

    private fun bumpGuide(key: DurableGuideKey) {
        execute(
            "UPDATE guide_state SET mutation_epoch = mutation_epoch + 1 WHERE source_key = ? AND feed_id = ?",
            key.sourceKey.value,
            key.feedId.value,
        )
    }

    private fun bumpSource(sourceKey: DurableGuideSourceKey) {
        ensureSource(sourceKey)
        execute(
            "UPDATE guide_source_state SET mutation_epoch = mutation_epoch + 1 WHERE source_key = ?",
            sourceKey.value,
        )
    }

    private fun sourceMutation(sourceKey: DurableGuideSourceKey): Long = queryLongOrNull(
        "SELECT mutation_epoch FROM guide_source_state WHERE source_key = ?",
        sourceKey.value,
    ) ?: 0L

    private fun adjustSourceCounts(
        sourceKey: DurableGuideSourceKey,
        activeDelta: Long = 0,
        stagedOnlyDelta: Long = 0,
    ) {
        if (activeDelta == 0L && stagedOnlyDelta == 0L) return
        execute(
            """
            UPDATE guide_source_state
            SET active_feed_count = active_feed_count + ?,
                staged_only_feed_count = staged_only_feed_count + ?
            WHERE source_key = ?
            """.trimIndent(),
            activeDelta,
            stagedOnlyDelta,
            sourceKey.value,
        )
        val source = sourceState(sourceKey) ?: throw DurableGuideStoreException.Corrupt()
        if (source.activeFeedCount < 0 || source.stagedOnlyFeedCount < 0) {
            throw DurableGuideStoreException.Corrupt()
        }
    }

    private fun cleanupEligible(now: Long): Boolean = queryLong(
        """
        SELECT EXISTS(
          SELECT 1 FROM guide_lease WHERE expires_at_ms <= ?
          UNION ALL SELECT 1 FROM guide_source_cleanup
          UNION ALL SELECT 1 FROM guide_generation WHERE status = 'staging' AND expires_at_ms <= ?
          UNION ALL
          SELECT 1 FROM guide_cleanup_queue q
          JOIN guide_generation g
            ON g.source_key = q.source_key AND g.feed_id = q.feed_id AND g.generation = q.generation
          WHERE NOT EXISTS (
            SELECT 1 FROM guide_lease l
            WHERE l.source_key = q.source_key AND l.feed_id = q.feed_id
              AND l.generation = q.generation AND l.expires_at_ms > ?
          )
        )
        """.trimIndent(),
        now,
        now,
        now,
    ) == 1L

    private fun changedRows(): Long = queryLong("SELECT changes()")

    private fun execute(sql: String, vararg arguments: Any?): Long = driver.execute(
        identifier = null,
        sql = sql,
        parameters = arguments.size,
        binders = { bind(arguments) },
    ).value

    private fun <T> query(sql: String, vararg arguments: Any?, mapper: (SqlCursor) -> T): List<T> =
        driver.executeQuery(
            identifier = null,
            sql = sql,
            mapper = { cursor ->
                val values = mutableListOf<T>()
                while (cursor.next().value) values += mapper(cursor)
                QueryResult.Value(values)
            },
            parameters = arguments.size,
            binders = { bind(arguments) },
        ).value

    private fun <T> queryOne(sql: String, vararg arguments: Any?, mapper: (SqlCursor) -> T): T? =
        query(sql, *arguments, mapper = mapper).singleOrNull()

    private fun queryLong(sql: String, vararg arguments: Any?): Long =
        queryLongOrNull(sql, *arguments) ?: throw DurableGuideStoreException.Corrupt()

    private fun queryLongOrNull(sql: String, vararg arguments: Any?): Long? =
        queryOne(sql, *arguments) { requiredLong(it, 0) }

    private fun explain(sql: String, vararg arguments: Any?): List<String> =
        query(sql, *arguments) { requiredString(it, 3) }

    private fun SqlPreparedStatement.bind(arguments: Array<out Any?>) {
        arguments.forEachIndexed { index, value ->
            when (value) {
                null -> bindString(index, null)
                is String -> bindString(index, value)
                is Long -> bindLong(index, value)
                is Int -> bindLong(index, value.toLong())
                else -> error("Unsupported guide SQL argument")
            }
        }
    }

    private suspend fun <T> read(block: () -> T): T = withContext(dispatcher) {
        currentCoroutineContext().ensureActive()
        try {
            block()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (known: DurableGuideStoreException) {
            throw known
        } catch (invalid: IllegalArgumentException) {
            throw invalid
        } catch (_: Throwable) {
            throw DurableGuideStoreException.Corrupt()
        }
    }

    private suspend fun <T> write(block: () -> T): T = writer.withLock { read(block) }
}

private const val STATUS_STAGING = "staging"
private const val STATUS_FAILED = "failed"
private const val STATUS_ABANDONED = "abandoned"
private const val STATUS_SUPERSEDED = "superseded"

private val guideJson = Json {
    encodeDefaults = true
    explicitNulls = true
    ignoreUnknownKeys = false
}

private fun encodeStrings(values: List<String>): String =
    guideJson.encodeToString(ListSerializer(String.serializer()), values)

private fun decodeStrings(value: String): List<String> = try {
    guideJson.decodeFromString(ListSerializer(String.serializer()), value)
} catch (_: Throwable) {
    throw DurableGuideStoreException.Corrupt()
}

private fun requiredLong(cursor: SqlCursor, index: Int): Long =
    cursor.getLong(index) ?: throw DurableGuideStoreException.Corrupt()

private fun requiredString(cursor: SqlCursor, index: Int): String =
    cursor.getString(index) ?: throw DurableGuideStoreException.Corrupt()

private fun checkedIncrement(value: Long): Long {
    require(value < Long.MAX_VALUE) { "Guide counter space is exhausted" }
    return value + 1
}

private fun checkedAdd(value: Long, increment: Long): Long {
    require(increment >= 0 && value <= Long.MAX_VALUE - increment) { "Guide counter space is exhausted" }
    return value + increment
}

private fun Long.toBoundedInt(): Int = coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

private data class GuideStateRow(
    val activeGeneration: Long?,
    val latestGeneration: Long?,
    val nextGeneration: Long,
    val revision: Long,
    val mutationEpoch: Long,
    val sourceEpoch: Long,
    val deleted: Boolean,
)

private data class GuideSourceStateRow(
    val mutationEpoch: Long,
    val deleteEpoch: Long,
    val activeFeedCount: Long,
    val stagedOnlyFeedCount: Long,
    val deleted: Boolean,
)

internal data class GuideCleanupWork(
    val expiredLeases: Int = 0,
    val retiredSourceFeeds: Int = 0,
    val expiredWriters: Int = 0,
    val removedPayloadRows: Int = 0,
) {
    val total: Int
        get() = expiredLeases + retiredSourceFeeds + expiredWriters + removedPayloadRows
}

private data class GenerationRow(
    val status: String,
    val writerEpoch: Long,
    val expiresAtMillis: Long,
    val retention: DurableGuideRetention,
    val attemptedBatches: Long,
    val attemptedChannelRows: Long,
    val attemptedProgrammeRows: Long,
    val channelCount: Long,
    val programmeCount: Long,
    val nextProgrammeOrdinal: Long,
    val cleanupStarted: Boolean,
)

private data class ProgrammeIdentity(
    val channelKey: DurableGuideChannelKey,
    val start: Instant,
)

private data class ProgrammeRow(
    val ordinal: Long,
    val record: DurableGuideProgrammeRecord,
)

private data class SearchResultRow(
    val channelKey: String,
    val startMillis: Long,
    val value: DurableGuideProgrammeSearchRow,
)

private data class CleanupTarget(
    val sourceKey: String,
    val feedId: String,
    val generation: Long,
)

private data class SourceCleanupJob(
    val sourceKey: String,
    val deleteEpoch: Long,
    val afterFeedId: String,
)

private data class LeaseRow(
    val ownerId: String,
    val sourceKey: String,
    val feedId: String,
    val generation: Long,
    val revision: Long,
    val expiresAtMillis: Long,
)

private class SqlGuideGeneration(
    val owner: Any,
    override val key: DurableGuideKey,
    val generation: Long,
    val mutationEpoch: Long,
) : DurableGuideGeneration {
    override fun toString(): String = "DurableGuideGeneration(key=<redacted>, generation=$generation)"
}

private class SqlGuideSnapshot(
    val owner: Any,
    override val key: DurableGuideKey,
    override val generation: Long,
    override val revision: Long,
    override val mutationEpoch: Long,
    override val counts: DurableGuideCounts,
    override val retention: DurableGuideRetention,
) : DurableGuideSnapshot {
    override fun toString(): String =
        "DurableGuideSnapshot(key=<redacted>, generation=$generation, revision=$revision, counts=$counts)"
}

private class SqlGuideSourceToken(
    val owner: Any,
    val sourceKey: DurableGuideSourceKey,
    val mutationEpoch: Long,
) : DurableGuideSourceToken {
    override fun toString(): String = "DurableGuideSourceToken(<redacted>)"
}

private class SqlGuideLease(
    val owner: Any,
    val leaseId: Long,
    val key: DurableGuideKey,
    val generation: Long,
    val revision: Long,
) : DurableGuideSnapshotLease {
    var valid: Boolean = true
    override fun toString(): String = "DurableGuideSnapshotLease(<redacted>)"
}

private class SqlGuideLocator(
    val owner: Any,
    val key: DurableGuideKey,
    val generation: Long,
    val revision: Long,
    val ordinal: Long,
) : DurableGuideProgrammeLocator {
    override fun toString(): String = "DurableGuideProgrammeLocator(<redacted>)"
}

private sealed interface GuideCursorDomain
private data class SourceCursorDomain(
    val sourceKey: DurableGuideSourceKey,
    val mutationEpoch: Long,
) : GuideCursorDomain
private data class ChannelCursorDomain(val leaseId: Long) : GuideCursorDomain
private data class SearchCursorDomain(val leaseId: Long) : GuideCursorDomain
private data class WindowCursorDomain(
    val leaseId: Long,
    val channelKey: DurableGuideChannelKey,
    val from: Instant,
    val until: Instant,
) : GuideCursorDomain

private class SqlGuideCursor(
    val owner: Any,
    val domain: GuideCursorDomain,
    val text: String?,
    val number: Long?,
) : DurableGuideCursor {
    override fun toString(): String = "DurableGuideCursor(<redacted>)"
}
