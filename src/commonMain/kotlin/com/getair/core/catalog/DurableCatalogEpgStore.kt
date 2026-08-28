package com.getair.core.catalog

import com.getair.iptv.EpgChannelIndex
import com.getair.iptv.EpgChannelMatchAtRevision
import com.getair.iptv.EpgChannelWindow
import com.getair.iptv.EpgFeedId
import com.getair.iptv.EpgGuideKey
import com.getair.iptv.EpgLimitKind
import com.getair.iptv.EpgNowNextResult
import com.getair.iptv.EpgProgrammeLocator
import com.getair.iptv.EpgProgrammeSearchProjection
import com.getair.iptv.EpgProgrammeSearchRow
import com.getair.iptv.EpgPruneResult
import com.getair.iptv.EpgQueryWork
import com.getair.iptv.EpgRefreshResult
import com.getair.iptv.EpgRetentionPolicy
import com.getair.iptv.EpgSnapshotInfo
import com.getair.iptv.EpgStore
import com.getair.iptv.EpgStoreException
import com.getair.iptv.EpgStoreLimits
import com.getair.iptv.EpgWindowResult
import com.getair.iptv.model.EpgBatch
import com.getair.iptv.model.EpgChannel
import com.getair.iptv.model.EpgChannelId
import com.getair.iptv.model.EpgFuzzyPolicy
import com.getair.iptv.model.EpgMatchOptions
import com.getair.iptv.model.EpgMatchResult
import com.getair.iptv.model.EpgNowNext
import com.getair.iptv.model.EpgProgramme
import com.getair.iptv.model.IptvGuide
import com.getair.iptv.model.IptvPlaylistEntry
import com.getair.iptv.model.PlaylistEntryId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.datetime.Instant
import kotlin.time.Duration.Companion.seconds

/**
 * Common durable [EpgStore] adapter. Provider identities are hashed before any
 * value reaches [DurableGuideStore]; provider URLs are either sanitized artwork
 * references or dropped.
 */
class DurableCatalogEpgStore(
    private val catalogs: DurableCatalogStore,
    private val retention: EpgRetentionPolicy = EpgRetentionPolicy(),
    private val limits: EpgStoreLimits = EpgStoreLimits(),
    private val ownsCatalogStore: Boolean = false,
) : EpgStore {
    private companion object {
        const val MAX_CHANNEL_CACHE_GUIDES = 32
    }
    private val durable: DurableGuideStore = catalogs.guides
    private val locatorOwner = Any()
    private val mutex = Mutex()
    private val tickets = mutableMapOf<EpgGuideKey, Long>()
    private val channelCaches = LinkedHashMap<EpgGuideKey, ChannelCache>()
    private var nextTicket = 0L
    private var closed = false

    init {
        require(durable.supported) { "Durable guide storage is unsupported" }
    }

    override suspend fun refresh(
        guide: EpgGuideKey,
        batches: Flow<EpgBatch>,
        retentionAnchor: Instant,
    ): EpgRefreshResult {
        ensureOpen()
        val key = guide.durableKey()
        val started = mutex.withLock {
            val value = ++nextTicket
            tickets[guide] = value
            try {
                value to durable.beginRefresh(
                    key,
                    DurableGuideRetention(
                        retentionAnchor,
                        retentionAnchor - retention.keepPastSeconds.seconds,
                        retentionAnchor + retention.keepFutureSeconds.seconds,
                    ),
                )
            } catch (failure: Throwable) {
                if (tickets[guide] == value) tickets.remove(guide)
                throw failure
            }
        }
        val ticket = started.first
        val generation = started.second
        val retainedFrom = retentionAnchor - retention.keepPastSeconds.seconds
        val retainedUntil = retentionAnchor + retention.keepFutureSeconds.seconds
        var terminal = false
        var batchesSeen = 0
        var inputChannels = 0L
        var inputProgrammes = 0L
        var outside = 0
        var invalid = 0
        var duplicates = 0
        var counts = DurableGuideCounts(0, 0)
        var durableBatchesSeen = 0
        var acceptedChannels = 0L
        var acceptedProgrammes = 0L
        var lastStageChannels = false
        var lastStageProgrammes = false
        try {
            batches.collect { batch ->
                currentCoroutineContext().ensureActive()
                batchesSeen++
                if (batchesSeen > limits.maxBatchesPerRefresh) limit(EpgLimitKind.Batches)
                if (batch.channels.size > limits.maxChannelsPerBatch) limit(EpgLimitKind.ChannelsPerBatch)
                if (batch.programmes.size > limits.maxProgrammesPerBatch) limit(EpgLimitKind.ProgrammesPerBatch)
                inputChannels += batch.channels.size
                inputProgrammes += batch.programmes.size
                if (inputChannels > limits.maxInputChannelsPerRefresh) limit(EpgLimitKind.InputChannels)
                if (inputProgrammes > limits.maxInputProgrammesPerRefresh) limit(EpgLimitKind.InputProgrammes)

                val channels = batch.channels.map { it.toDurable(guide) }
                val programmes = ArrayList<DurableGuideProgrammeRecord>(batch.programmes.size)
                batch.programmes.forEach { programme ->
                    if (programme.title.isBlank() || programme.end?.let { it <= programme.start } == true) {
                        invalid++
                    } else if (!(programme.start < retainedUntil && (programme.end ?: Instant.DISTANT_FUTURE) > retainedFrom)) {
                        outside++
                    } else {
                        programmes += programme.toDurable(guide)
                    }
                }
                val pending = ArrayList<PendingRow>(channels.size + programmes.size)
                channels.forEach { pending += PendingRow.Channel(it) }
                programmes.forEach { pending += PendingRow.Programme(it) }
                pending.chunked(DurableGuideLimits.MAX_BATCH_ITEMS).forEach { chunk ->
                    val stagedChannels = chunk.mapNotNull { (it as? PendingRow.Channel)?.value }
                    val stagedProgrammes = chunk.mapNotNull { (it as? PendingRow.Programme)?.value }
                    durableBatchesSeen++
                    acceptedChannels += stagedChannels.size
                    acceptedProgrammes += stagedProgrammes.size
                    lastStageChannels = stagedChannels.isNotEmpty()
                    lastStageProgrammes = stagedProgrammes.isNotEmpty()
                    val before = counts
                    counts = durable.stage(generation, stagedChannels, stagedProgrammes)
                    duplicates += stagedProgrammes.size - (counts.programmes - before.programmes).toInt()
                    if (counts.channels > limits.maxStoredChannelsPerGuide) limit(EpgLimitKind.StoredChannels)
                    if (counts.programmes > limits.maxStoredProgrammesPerGuide) limit(EpgLimitKind.StoredProgrammes)
                }
            }
            currentCoroutineContext().ensureActive()
            if (counts.channels == 0L && counts.programmes == 0L) throw EpgStoreException.EmptyRefresh()
            if (!mutex.withLock { tickets[guide] == ticket }) {
                terminal = durable.abandon(generation)
                return result(false, null, counts, outside, invalid, duplicates)
            }
            val activation = durable.activate(generation, counts)
            terminal = true
            return when (activation) {
                is DurableGuideActivation.Published -> {
                    mutex.withLock {
                        if (tickets[guide] == ticket) tickets.remove(guide)
                        channelCaches.remove(guide)
                    }
                    result(true, activation.snapshot.revision, counts, outside, invalid, duplicates)
                }
                is DurableGuideActivation.Superseded ->
                    result(false, null, counts, outside, invalid, duplicates)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (known: EpgStoreException) {
            throw known
        } catch (_: DurableGuideStoreException.Stale) {
            if (!mutex.withLock { tickets[guide] == ticket }) {
                return result(false, null, counts, outside, invalid, duplicates)
            }
            throw EpgStoreException.RefreshFailed()
        } catch (_: DurableGuideStoreException.Limit) {
            val kind = when {
                durableBatchesSeen > DurableGuideLimits.MAX_GENERATION_BATCHES -> EpgLimitKind.Batches
                acceptedChannels > DurableGuideLimits.MAX_INPUT_CHANNEL_ROWS -> EpgLimitKind.InputChannels
                acceptedProgrammes > DurableGuideLimits.MAX_INPUT_PROGRAMME_ROWS -> EpgLimitKind.InputProgrammes
                lastStageChannels &&
                    counts.channels >= DurableGuideLimits.MAX_GENERATION_CHANNELS - DurableGuideLimits.MAX_BATCH_ITEMS ->
                    EpgLimitKind.StoredChannels
                lastStageProgrammes -> EpgLimitKind.StoredProgrammes
                lastStageChannels -> EpgLimitKind.StoredChannels
                else -> EpgLimitKind.Batches
            }
            throw EpgStoreException.LimitExceeded(kind)
        } catch (_: Exception) {
            throw EpgStoreException.RefreshFailed()
        } finally {
            if (!terminal) {
                withContext(NonCancellable) { durable.abandon(generation) }
            }
            mutex.withLock { if (tickets[guide] == ticket) tickets.remove(guide) }
        }
    }

    override suspend fun snapshotInfo(guide: EpgGuideKey): EpgSnapshotInfo? {
        ensureOpen()
        val snapshot = durable.snapshot(guide.durableKey()) ?: return null
        return EpgSnapshotInfo(
            snapshot.revision,
            snapshot.counts.channels.toInt(),
            snapshot.counts.programmes.toInt(),
            snapshot.retention.anchor,
        )
    }

    override suspend fun programmeSearchProjection(
        guide: EpgGuideKey,
        expectedRevision: Long?,
    ): EpgProgrammeSearchProjection? {
        ensureOpen()
        val snapshot = durable.snapshot(guide.durableKey()) ?: return null
        if (expectedRevision != null && snapshot.revision != expectedRevision) return null
        return EpgProgrammeSearchProjection(
            guide,
            snapshot.revision,
            snapshot.retention.anchor,
            snapshot.retention.retainedFrom,
            snapshot.retention.retainedUntil,
            snapshot.counts.programmes.toInt(),
            projectionRows(guide, snapshot),
        )
    }

    override suspend fun programme(
        guide: EpgGuideKey,
        locator: EpgProgrammeLocator,
        expectedRevision: Long,
    ): EpgProgramme? {
        ensureOpen()
        val local = locator as? AdapterProgrammeLocator ?: return null
        if (!local.matches(locatorOwner, guide, expectedRevision)) return null
        val snapshot = durable.snapshot(guide.durableKey()) ?: return null
        if (snapshot.revision != expectedRevision) return null
        val lease = durable.acquire(snapshot) ?: return null
        return try {
            durable.programme(lease, local.durable)?.toEpg()
        } finally {
            withContext(NonCancellable) { durable.release(lease) }
        }
    }

    override suspend fun matchChannel(
        guide: EpgGuideKey,
        entry: IptvPlaylistEntry,
        options: EpgMatchOptions,
    ): EpgChannelMatchAtRevision {
        ensureOpen()
        val snapshot = durable.snapshot(guide.durableKey())
            ?: return EpgChannelMatchAtRevision(null, EpgMatchResult.Unmatched)
        val cache = channels(guide, snapshot, options.fuzzyPolicy != EpgFuzzyPolicy.Disabled)
            ?: return EpgChannelMatchAtRevision(null, EpgMatchResult.Unmatched)
        val transformed = entry.copy(
            id = PlaylistEntryId(hash("air-epg-entry-v1", guide.sourceId.value, entry.id.value)),
            epgChannelId = entry.epgChannelId?.let { EpgChannelId(guide.channelHandle(it.value)) },
        )
        val overrides = buildMap {
            options.overrides.forEach { (rawKey, rawValue) ->
                val value = EpgChannelId(guide.channelHandle(rawValue.value))
                put(hash("air-epg-entry-v1", guide.sourceId.value, rawKey), value)
                put(guide.channelHandle(rawKey), value)
            }
        }
        val index = if (options.fuzzyPolicy == EpgFuzzyPolicy.Disabled) cache.exact else cache.fuzzy
        return EpgChannelMatchAtRevision(
            snapshot.revision,
            index.match(transformed, options.copy(overrides = overrides)),
        )
    }

    override suspend fun nowNext(
        guide: EpgGuideKey,
        channelId: EpgChannelId,
        at: Instant,
    ): EpgNowNextResult {
        ensureOpen()
        val snapshot = durable.snapshot(guide.durableKey())
            ?: return EpgNowNextResult(EpgNowNext(null, null), null, EpgQueryWork(1, 0, 0, 0))
        val lease = durable.acquire(snapshot) ?: return EpgNowNextResult(
            EpgNowNext(null, null), null, EpgQueryWork(1, 0, 0, 0),
        )
        return try {
            val value = durable.nowNext(lease, DurableGuideChannelKey(guide.channelDigest(channelId.value)), at)
            EpgNowNextResult(
                EpgNowNext(
                    value.current?.toEpg(channelId),
                    value.next?.toEpg(channelId),
                ),
                snapshot.revision,
                EpgQueryWork(1, 1, 0, listOfNotNull(value.current, value.next).size),
            )
        } finally {
            withContext(NonCancellable) { durable.release(lease) }
        }
    }

    override suspend fun visibleWindow(
        guide: EpgGuideKey,
        channelIds: List<EpgChannelId>,
        start: Instant,
        endExclusive: Instant,
    ): EpgWindowResult {
        ensureOpen()
        if (endExclusive <= start) throw EpgStoreException.InvalidWindow()
        if (channelIds.size > limits.maxChannelsPerWindowQuery) limit(EpgLimitKind.QueryChannels)
        val snapshot = durable.snapshot(guide.durableKey())
            ?: return EpgWindowResult(emptyList(), null, false, EpgQueryWork(1, 0, 0, 0))
        val unique = channelIds.distinct()
        if (unique.isEmpty()) {
            return EpgWindowResult(emptyList(), snapshot.revision, false, EpgQueryWork(1, 0, 0, 0))
        }
        val lease = durable.acquire(snapshot)
            ?: return EpgWindowResult(emptyList(), null, false, EpgQueryWork(1, 0, 0, 0))
        return try {
            val request = DurableGuideMultiChannelWindowRequest(
                unique.map { DurableGuideChannelKey(guide.channelDigest(it.value)) },
                start,
                endExclusive,
                perChannelLimit = minOf(limits.maxProgrammesPerWindowQuery, DurableGuideLimits.MAX_WINDOW_ITEMS),
                totalLimit = minOf(limits.maxProgrammesPerWindowQuery, DurableGuideLimits.MAX_MULTI_WINDOW_ITEMS),
            )
            val value = durable.multiChannelWindow(lease, request)
            val cache = channels(guide, snapshot, false)
            var examined = 0
            val rows = value.channels.mapIndexed { index, channel ->
                val requested = unique[index]
                val programmes = channel.programmes.map { it.toEpg(requested) }
                examined += programmes.size
                EpgChannelWindow(
                    requested,
                    cache?.byKey?.get(channel.channelKey)?.copy(id = requested),
                    programmes,
                )
            }
            EpgWindowResult(
                rows,
                snapshot.revision,
                value.truncated,
                EpgQueryWork(1, unique.size, 0, examined),
            )
        } finally {
            withContext(NonCancellable) { durable.release(lease) }
        }
    }

    override suspend fun prune(guide: EpgGuideKey, retentionAnchor: Instant): EpgPruneResult {
        ensureOpen()
        val key = guide.durableKey()
        val snapshot = durable.snapshot(key) ?: return EpgPruneResult(false, null, 0, 0)
        val lower = maxOf(
            snapshot.retention.retainedFrom,
            retentionAnchor - retention.keepPastSeconds.seconds,
        )
        val upper = minOf(
            snapshot.retention.retainedUntil,
            retentionAnchor + retention.keepFutureSeconds.seconds,
        )
        if (lower >= upper) {
            val removed = snapshot.counts.programmes.toInt()
            return when (val deleted = durable.deleteGuide(key, snapshot.revision, snapshot.mutationEpoch)) {
                is DurableGuideDeleteResult.Deleted -> {
                    mutex.withLock { channelCaches.remove(guide) }
                    EpgPruneResult(true, deleted.revision, removed, 0)
                }
                is DurableGuideDeleteResult.Superseded -> {
                    val current = deleted.current
                    EpgPruneResult(
                        current != null,
                        current?.revision,
                        0,
                        current?.counts?.programmes?.toInt() ?: 0,
                    )
                }
            }
        }
        return when (
            val result = durable.prune(
                key,
                snapshot.revision,
                snapshot.mutationEpoch,
                DurableGuideRetention(snapshot.retention.anchor, lower, upper),
            )
        ) {
            is DurableGuidePruneResult.Published -> {
                mutex.withLock { channelCaches.remove(guide) }
                EpgPruneResult(
                    true,
                    result.snapshot.revision,
                    (snapshot.counts.programmes - result.snapshot.counts.programmes).toInt(),
                    result.snapshot.counts.programmes.toInt(),
                )
            }
            is DurableGuidePruneResult.Unchanged ->
                EpgPruneResult(true, result.current.revision, 0, result.current.counts.programmes.toInt())
            is DurableGuidePruneResult.Superseded -> {
                val current = result.current
                EpgPruneResult(current != null, current?.revision, 0, current?.counts?.programmes?.toInt() ?: 0)
            }
        }
    }

    override suspend fun remove(guide: EpgGuideKey): Boolean {
        ensureOpen()
        val key = guide.durableKey()
        val snapshot = durable.snapshot(key) ?: return false
        val result = durable.deleteGuide(key, snapshot.revision, snapshot.mutationEpoch)
        mutex.withLock {
            tickets.remove(guide)
            channelCaches.remove(guide)
        }
        return result is DurableGuideDeleteResult.Deleted
    }

    fun close() {
        if (closed) return
        closed = true
        channelCaches.clear()
        tickets.clear()
        if (ownsCatalogStore) catalogs.close()
    }

    internal suspend fun channelCacheSize(): Int = mutex.withLock { channelCaches.size }

    private fun projectionRows(
        guide: EpgGuideKey,
        snapshot: DurableGuideSnapshot,
    ): Flow<EpgProgrammeSearchRow> = flow {
        val lease = durable.acquire(snapshot) ?: throw EpgStoreException.RefreshFailed()
        try {
            var cursor: DurableGuideCursor? = null
            do {
                currentCoroutineContext().ensureActive()
                if (!durable.renew(lease)) throw EpgStoreException.RefreshFailed()
                val page = durable.programmeSearchRows(lease, cursor, DurableGuideLimits.MAX_PAGE_ITEMS)
                page.rows.forEach { row ->
                    emit(
                        EpgProgrammeSearchRow(
                            AdapterProgrammeLocator(locatorOwner, guide, snapshot.revision, row.locator),
                            row.start,
                            row.effectiveEnd,
                            row.title,
                            row.subtitle,
                        ),
                    )
                }
                cursor = page.nextCursor
            } while (cursor != null)
        } finally {
            withContext(NonCancellable) { durable.release(lease) }
        }
    }

    private suspend fun channels(
        guide: EpgGuideKey,
        snapshot: DurableGuideSnapshot,
        fuzzy: Boolean,
    ): ChannelCache? {
        mutex.withLock {
            channelCaches.remove(guide)?.also { channelCaches[guide] = it }
        }
            ?.takeIf { it.revision == snapshot.revision && (!fuzzy || it.fuzzyReady) }
            ?.let { return it }
        val lease = durable.acquire(snapshot) ?: return null
        val rows = ArrayList<EpgChannel>(snapshot.counts.channels.toInt())
        try {
            var cursor: DurableGuideCursor? = null
            do {
                if (!durable.renew(lease)) return null
                val page = durable.channels(lease, cursor, DurableGuideLimits.MAX_PAGE_ITEMS)
                page.channels.forEach { channel ->
                    rows += channel.toEpg()
                }
                cursor = page.nextCursor
            } while (cursor != null)
        } finally {
            withContext(NonCancellable) { durable.release(lease) }
        }
        val exact = EpgChannelIndex.build(IptvGuide(rows, emptyList()), enableFuzzy = false)
        val fuzzyIndex = if (fuzzy) EpgChannelIndex.build(IptvGuide(rows, emptyList()), enableFuzzy = true) else exact
        val cache = ChannelCache(
            snapshot.revision,
            rows.associateBy { DurableGuideChannelKey(channelDigestFromHandle(it.id.value)) },
            exact,
            fuzzyIndex,
            fuzzy,
        )
        return mutex.withLock {
            val current = durable.snapshot(guide.durableKey())
            if (current?.revision != snapshot.revision) null else {
                val old = channelCaches[guide]
                if (old == null || old.revision != snapshot.revision || (fuzzy && !old.fuzzyReady)) {
                    channelCaches[guide] = cache
                    while (channelCaches.size > MAX_CHANNEL_CACHE_GUIDES) {
                        channelCaches.remove(channelCaches.keys.first())
                    }
                    cache
                } else old
            }
        }
    }

    private fun ensureOpen() {
        check(!closed) { "Durable EPG store is closed" }
    }

    private fun limit(kind: EpgLimitKind): Nothing = throw EpgStoreException.LimitExceeded(kind)

    private sealed interface PendingRow {
        class Channel(val value: DurableGuideChannelRecord) : PendingRow
        class Programme(val value: DurableGuideProgrammeRecord) : PendingRow
    }

    private class AdapterProgrammeLocator(
        private val owner: Any,
        private val guide: EpgGuideKey,
        private val revision: Long,
        val durable: DurableGuideProgrammeLocator,
    ) : EpgProgrammeLocator {
        fun matches(candidateOwner: Any, candidateGuide: EpgGuideKey, candidateRevision: Long): Boolean =
            owner === candidateOwner && guide == candidateGuide && revision == candidateRevision

        override fun toString(): String = "EpgProgrammeLocator(<redacted>)"
    }

    private data class ChannelCache(
        val revision: Long,
        val byKey: Map<DurableGuideChannelKey, EpgChannel>,
        val exact: EpgChannelIndex,
        val fuzzy: EpgChannelIndex,
        val fuzzyReady: Boolean,
    )
}

private fun EpgGuideKey.durableKey(): DurableGuideKey = DurableGuideKey(
    DurableGuideSourceKey(hash("air-guide-source-v1", sourceId.value)),
    DurableGuideFeedId(feedId.value),
)

private fun EpgGuideKey.channelDigest(rawChannelId: String): String =
    if (isChannelHandle(rawChannelId)) {
        channelDigestFromHandle(rawChannelId)
    } else {
        hash("air-guide-channel-v1", sourceId.value, feedId.value, rawChannelId)
    }

private fun EpgGuideKey.channelHandle(rawChannelId: String): String =
    if (isChannelHandle(rawChannelId)) rawChannelId else CHANNEL_HANDLE_PREFIX + channelDigest(rawChannelId)

private fun EpgChannel.toDurable(guide: EpgGuideKey): DurableGuideChannelRecord =
    DurableGuideChannelRecord(
        DurableGuideChannelKey(guide.channelDigest(id.value)),
        displayNames = displayNames.take(DurableGuideLimits.MAX_DISPLAY_NAMES).map {
            bounded(it, DurableGuideLimits.MAX_DISPLAY_NAME_CHARS)
        }.filter(String::isNotBlank).ifEmpty { listOf("Channel") },
        artworkReference = safeArtwork(iconUrl),
    )

private fun EpgProgramme.toDurable(guide: EpgGuideKey): DurableGuideProgrammeRecord =
    DurableGuideProgrammeRecord(
        channelKey = DurableGuideChannelKey(guide.channelDigest(channelId.value)),
        start = start,
        end = end,
        title = bounded(title, DurableGuideLimits.MAX_TITLE_CHARS),
        subtitle = subtitle?.let { bounded(it, DurableGuideLimits.MAX_SUBTITLE_CHARS) },
        description = description?.let { bounded(it, DurableGuideLimits.MAX_DESCRIPTION_CHARS) },
        categories = categories.take(DurableGuideLimits.MAX_CATEGORIES).map {
            bounded(it, DurableGuideLimits.MAX_CATEGORY_CHARS)
        }.filter(String::isNotBlank),
        episode = episode?.let { bounded(it, DurableGuideLimits.MAX_EPISODE_CHARS) },
        artworkReference = safeArtwork(iconUrl),
    )

private fun DurableGuideChannelRecord.toEpg(): EpgChannel = EpgChannel(
    EpgChannelId(CHANNEL_HANDLE_PREFIX + key.value),
    displayNames,
    artworkReference,
    emptyList(),
)

private fun DurableGuideProgrammeRecord.toEpg(
    channelId: EpgChannelId = EpgChannelId(CHANNEL_HANDLE_PREFIX + channelKey.value),
): EpgProgramme = EpgProgramme(
    channelId,
    start,
    end,
    title,
    subtitle,
    description,
    categories,
    artworkReference,
    episode,
)

private fun result(
    committed: Boolean,
    revision: Long?,
    counts: DurableGuideCounts,
    outside: Int,
    invalid: Int,
    duplicates: Int,
): EpgRefreshResult = EpgRefreshResult(
    committed,
    revision,
    counts.channels.toInt(),
    counts.programmes.toInt(),
    outside,
    invalid,
    duplicates,
)

private fun safeArtwork(value: String?): String? {
    val candidate = value?.trim()?.takeIf(String::isNotEmpty) ?: return null
    val lower = candidate.lowercase()
    if ((!lower.startsWith("https://") && !lower.startsWith("http://")) || '?' in lower || '#' in lower) return null
    val scheme = lower.indexOf("://")
    val end = lower.indexOf('/', scheme + 3).let { if (it < 0) lower.length else it }
    val authority = lower.substring(scheme + 3, end)
    return candidate.takeIf { authority.isNotBlank() && '@' !in authority && authority.none(Char::isWhitespace) }
}

private fun bounded(value: String, maxChars: Int): String {
    var end = minOf(value.length, maxChars)
    if (end in 1 until value.length && value[end - 1].isHighSurrogate() && value[end].isLowSurrogate()) end--
    return value.substring(0, end)
}

private fun Char.isHighSurrogate(): Boolean = code in 0xD800..0xDBFF
private fun Char.isLowSurrogate(): Boolean = code in 0xDC00..0xDFFF

private fun hash(domain: String, vararg values: String): String = sha256Hex(
    buildString {
        append(domain.length).append(':').append(domain).append(';')
        values.forEach { value -> append(value.encodeToByteArray().size).append(':').append(value).append(';') }
    }.encodeToByteArray(),
)

private const val CHANNEL_HANDLE_PREFIX = "air-epg:"

private fun isChannelHandle(value: String): Boolean =
    value.startsWith(CHANNEL_HANDLE_PREFIX) &&
        value.removePrefix(CHANNEL_HANDLE_PREFIX).length == DurableGuideLimits.OPAQUE_DIGEST_CHARS &&
        value.removePrefix(CHANNEL_HANDLE_PREFIX).all { it in '0'..'9' || it in 'a'..'f' }

private fun channelDigestFromHandle(value: String): String =
    value.removePrefix(CHANNEL_HANDLE_PREFIX).takeIf {
        it.length == DurableGuideLimits.OPAQUE_DIGEST_CHARS && it.all { char -> char in '0'..'9' || char in 'a'..'f' }
    } ?: error("Invalid durable channel handle")
