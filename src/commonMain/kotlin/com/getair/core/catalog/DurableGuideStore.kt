package com.getair.core.catalog

import kotlinx.datetime.Instant

/** Stable limits shared by guide ingestors and durable backend implementations. */
object DurableGuideLimits {
    const val MAX_BATCH_ITEMS: Int = 256
    const val MAX_GENERATION_BATCHES: Int = 8_192
    const val MAX_INPUT_CHANNEL_ROWS: Long = 100_000
    const val MAX_INPUT_PROGRAMME_ROWS: Long = 1_000_000
    const val MAX_GENERATION_CHANNELS: Long = 50_000
    const val MAX_GENERATION_PROGRAMMES: Long = 500_000
    const val MAX_PAGE_ITEMS: Int = 200
    const val MAX_WINDOW_ITEMS: Int = 1_000
    const val MAX_MULTI_WINDOW_CHANNELS: Int = 512
    const val MAX_MULTI_WINDOW_ITEMS: Int = 20_000
    const val MAX_MULTI_WINDOW_PAYLOAD_BYTES: Int = 8 * 1_024 * 1_024
    const val MAX_LIVE_LEASES: Int = 128
    const val MAX_CLEANUP_ROWS: Int = 10_000
    const val MAX_FEED_ID_CHARS: Int = 128
    const val OPAQUE_DIGEST_CHARS: Int = 64
    const val MAX_DISPLAY_NAMES: Int = 8
    const val MAX_DISPLAY_NAME_CHARS: Int = 256
    const val MAX_TITLE_CHARS: Int = 512
    const val MAX_SUBTITLE_CHARS: Int = 256
    const val MAX_DESCRIPTION_CHARS: Int = 8_192
    const val MAX_EPISODE_CHARS: Int = 256
    const val MAX_ARTWORK_REFERENCE_CHARS: Int = 8_192
    const val MAX_CATEGORIES: Int = 32
    const val MAX_CATEGORY_CHARS: Int = 128
    const val DEFAULT_CLEANUP_ROWS: Int = 512
    const val DEFAULT_LEASE_IDLE_TIMEOUT_MILLIS: Long = 30_000
    const val DEFAULT_GENERATION_IDLE_TIMEOUT_MILLIS: Long = 60_000
    const val MAX_LEASE_IDLE_TIMEOUT_MILLIS: Long = 5 * 60_000
    const val MAX_GENERATION_IDLE_TIMEOUT_MILLIS: Long = 10 * 60_000
}

/** App-generated SHA-256 identity replacing a raw application/provider source ID. */
class DurableGuideSourceKey(val value: String) {
    init { requireDigest(value, "Guide source key") }
    override fun equals(other: Any?): Boolean = other is DurableGuideSourceKey && value == other.value
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "DurableGuideSourceKey(<redacted>)"
}

/** App-owned, opaque identity for one guide feed. Never use a provider URL or XMLTV ID. */
class DurableGuideFeedId(val value: String) {
    init { requireOpaque(value, DurableGuideLimits.MAX_FEED_ID_CHARS, "Guide feed ID") }

    override fun equals(other: Any?): Boolean = other is DurableGuideFeedId && value == other.value
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "DurableGuideFeedId(<redacted>)"
}

/** A source may own any number of independently refreshed guide feeds. */
class DurableGuideKey(
    val sourceKey: DurableGuideSourceKey,
    val feedId: DurableGuideFeedId,
) {
    override fun equals(other: Any?): Boolean =
        other is DurableGuideKey && sourceKey == other.sourceKey && feedId == other.feedId

    override fun hashCode(): Int = 31 * sourceKey.hashCode() + feedId.hashCode()
    override fun toString(): String = "DurableGuideKey(source=<redacted>, feed=<redacted>)"
}

/** Retained half-open guide interval and the refresh anchor used to derive it. */
data class DurableGuideRetention(
    val anchor: Instant,
    val retainedFrom: Instant,
    val retainedUntil: Instant,
) {
    init { require(retainedFrom < retainedUntil) { "Guide retention interval must be non-empty" } }

    override fun toString(): String =
        "DurableGuideRetention(anchor=$anchor, retainedFrom=$retainedFrom, retainedUntil=$retainedUntil)"
}

/** Backend-owned unpublished generation token. Implementations must keep [toString] redacted. */
interface DurableGuideGeneration {
    val key: DurableGuideKey
}

/** Safe, app-generated identity replacing raw XMLTV channel IDs. */
class DurableGuideChannelKey(val value: String) {
    init { requireDigest(value, "Guide channel key") }
    override fun equals(other: Any?): Boolean = other is DurableGuideChannelKey && value == other.value
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "DurableGuideChannelKey(<redacted>)"
}

/**
 * Stable SHA-256 identity of canonical sanitized content. It is not a ranking key;
 * duplicate selection must use [DurableGuideWinnerPolicy].
 */
class DurableGuideWinnerKey internal constructor(val value: String) {
    init { requireDigest(value, "Guide winner key") }
    override fun equals(other: Any?): Boolean = other is DurableGuideWinnerKey && value == other.value
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "DurableGuideWinnerKey(<redacted>)"
}

/** Field-order policy matching IPTV's portable in-memory EPG comparator after sanitization. */
object DurableGuideWinnerPolicy {
    fun compareChannels(left: DurableGuideChannelRecord, right: DurableGuideChannelRecord): Int {
        var comparison = compareStrings(left.displayNames, right.displayNames)
        if (comparison != 0) return comparison
        return compareNullable(left.artworkReference, right.artworkReference)
    }

    fun compareProgrammes(left: DurableGuideProgrammeRecord, right: DurableGuideProgrammeRecord): Int {
        var comparison = compareNullable(left.end, right.end)
        if (comparison != 0) return comparison
        comparison = left.title.compareTo(right.title)
        if (comparison != 0) return comparison
        comparison = compareNullable(left.subtitle, right.subtitle)
        if (comparison != 0) return comparison
        comparison = compareNullable(left.description, right.description)
        if (comparison != 0) return comparison
        comparison = compareStrings(left.categories, right.categories)
        if (comparison != 0) return comparison
        comparison = compareNullable(left.artworkReference, right.artworkReference)
        if (comparison != 0) return comparison
        return compareNullable(left.episode, right.episode)
    }

    private fun compareStrings(left: List<String>, right: List<String>): Int {
        repeat(minOf(left.size, right.size)) { index ->
            val comparison = left[index].compareTo(right[index])
            if (comparison != 0) return comparison
        }
        return left.size.compareTo(right.size)
    }

    private fun <T : Comparable<T>> compareNullable(left: T?, right: T?): Int = when {
        left == null && right == null -> 0
        left == null -> -1
        right == null -> 1
        else -> left.compareTo(right)
    }
}

/** The only supported winner derivation for both in-memory and durable guide stores. */
object DurableGuideWinnerKeys {
    fun channel(
        displayNames: List<String>,
        artworkReference: String?,
    ): DurableGuideWinnerKey = winnerKey(
        "channel",
        displayNames,
        listOf(artworkReference),
    )

    fun programme(
        channelKey: DurableGuideChannelKey,
        start: Instant,
        end: Instant?,
        title: String,
        subtitle: String?,
        description: String?,
        categories: List<String>,
        episode: String?,
        artworkReference: String?,
    ): DurableGuideWinnerKey = winnerKey(
        "programme",
        categories,
        listOf(
            channelKey.value,
            start.toEpochMilliseconds().toString(),
            end?.toEpochMilliseconds()?.toString(),
            title,
            subtitle,
            description,
            episode,
            artworkReference,
        ),
    )

    private fun winnerKey(
        kind: String,
        repeated: List<String>,
        fields: List<String?>,
    ): DurableGuideWinnerKey {
        val canonical = buildString {
            appendCanonicalField("air-guide-winner-v1")
            appendCanonicalField(kind)
            append(repeated.size).append(';')
            repeated.forEach { appendCanonicalField(it) }
            append(fields.size).append(';')
            fields.forEach { appendCanonicalField(it) }
        }
        return DurableGuideWinnerKey(sha256Hex(canonical.encodeToByteArray()))
    }
}

/**
 * Safe channel metadata. Ingestors omit artwork that does not pass the plain HTTP(S)
 * reference boundary before constructing this record.
 */
class DurableGuideChannelRecord(
    val key: DurableGuideChannelKey,
    displayNames: List<String>,
    artworkReference: String? = null,
) {
    val displayNames: List<String> = displayNames.toList()
    val artworkReference: String? = artworkReference?.trim()
    /** SHA-256 identity of canonical sanitized channel content. */
    val winnerKey: DurableGuideWinnerKey = DurableGuideWinnerKeys.channel(
        this.displayNames,
        this.artworkReference,
    )

    init {
        require(this.displayNames.isNotEmpty() && this.displayNames.size <= DurableGuideLimits.MAX_DISPLAY_NAMES)
        require(this.displayNames.all { it.isNotBlank() && it.length <= DurableGuideLimits.MAX_DISPLAY_NAME_CHARS })
        this.artworkReference?.requireSafeArtworkReference()
    }

    override fun toString(): String = "DurableGuideChannelRecord(key=<redacted>, metadata=<redacted>)"
}

/**
 * Full safe programme payload. Provider event/channel IDs and guide source URLs are absent;
 * optional artwork has already crossed the credential-safe reference boundary.
 */
class DurableGuideProgrammeRecord(
    val channelKey: DurableGuideChannelKey,
    val start: Instant,
    val end: Instant?,
    title: String,
    subtitle: String? = null,
    description: String? = null,
    categories: List<String> = emptyList(),
    episode: String? = null,
    artworkReference: String? = null,
) {
    val effectiveEnd: Instant = end ?: Instant.DISTANT_FUTURE
    val title: String = title
    val subtitle: String? = subtitle
    val description: String? = description
    val categories: List<String> = categories.toList()
    val episode: String? = episode
    val artworkReference: String? = artworkReference?.trim()
    val winnerKey: DurableGuideWinnerKey = DurableGuideWinnerKeys.programme(
        channelKey,
        start,
        end,
        this.title,
        this.subtitle,
        this.description,
        this.categories,
        this.episode,
        this.artworkReference,
    )

    init {
        require(effectiveEnd > start) { "Programme end must follow its start" }
        require(this.title.isNotBlank() && this.title.length <= DurableGuideLimits.MAX_TITLE_CHARS)
        require(this.subtitle == null || this.subtitle.length <= DurableGuideLimits.MAX_SUBTITLE_CHARS)
        require(this.description == null || this.description.length <= DurableGuideLimits.MAX_DESCRIPTION_CHARS)
        require(this.episode == null || this.episode.length <= DurableGuideLimits.MAX_EPISODE_CHARS)
        require(this.categories.size <= DurableGuideLimits.MAX_CATEGORIES)
        require(this.categories.all { it.isNotBlank() && it.length <= DurableGuideLimits.MAX_CATEGORY_CHARS })
        this.artworkReference?.requireSafeArtworkReference()
    }

    override fun toString(): String =
        "DurableGuideProgrammeRecord(channel=<redacted>, winner=<redacted>, metadata=<redacted>)"
}

data class DurableGuideCounts(val channels: Long, val programmes: Long) {
    init {
        require(channels in 0..DurableGuideLimits.MAX_GENERATION_CHANNELS)
        require(programmes in 0..DurableGuideLimits.MAX_GENERATION_PROGRAMMES)
    }
}

/** Backend-owned immutable active-generation token. Reads are bound to its [revision]; rendering is redacted. */
interface DurableGuideSnapshot {
    val key: DurableGuideKey
    val generation: Long
    val revision: Long
    /** Changes for refresh/prune/delete intent even before active content changes. */
    val mutationEpoch: Long
    val counts: DurableGuideCounts
    val retention: DurableGuideRetention
}

sealed interface DurableGuideActivation {
    class Published(val snapshot: DurableGuideSnapshot) : DurableGuideActivation {
        override fun toString(): String =
            "DurableGuideActivation.Published(snapshot=<redacted>, revision=${snapshot.revision})"
    }

    class Superseded(val current: DurableGuideSnapshot?) : DurableGuideActivation {
        override fun toString(): String =
            "DurableGuideActivation.Superseded(current=<redacted>, revision=${current?.revision})"
    }
}

/** Backend-owned, operation-bound continuation; implementations must keep [toString] redacted. */
interface DurableGuideCursor

/** Backend-owned O(1)/indexed programme identity; implementations must keep [toString] redacted. */
interface DurableGuideProgrammeLocator

/** Backend-owned generation pin; implementations must keep [toString] redacted. */
interface DurableGuideSnapshotLease

class DurableGuideSnapshotPage(
    snapshots: List<DurableGuideSnapshot>,
    val nextCursor: DurableGuideCursor?,
) {
    val snapshots: List<DurableGuideSnapshot> = snapshots.toList()
}

class DurableGuideChannelPage(
    channels: List<DurableGuideChannelRecord>,
    val nextCursor: DurableGuideCursor?,
) {
    val channels: List<DurableGuideChannelRecord> = channels.toList()
}

/** Compact search projection. Descriptions, categories, artwork, and channel identities are absent. */
class DurableGuideProgrammeSearchRow(
    val locator: DurableGuideProgrammeLocator,
    val start: Instant,
    val effectiveEnd: Instant,
    title: String,
    subtitle: String?,
) {
    val title: String = title
    val subtitle: String? = subtitle

    init {
        require(effectiveEnd > start)
        require(this.title.isNotBlank() && this.title.length <= DurableGuideLimits.MAX_TITLE_CHARS)
        require(this.subtitle == null || this.subtitle.length <= DurableGuideLimits.MAX_SUBTITLE_CHARS)
    }

    override fun toString(): String = "DurableGuideProgrammeSearchRow(locator=<redacted>, metadata=<redacted>)"
}

class DurableGuideProgrammeSearchPage(
    rows: List<DurableGuideProgrammeSearchRow>,
    val nextCursor: DurableGuideCursor?,
) {
    val rows: List<DurableGuideProgrammeSearchRow> = rows.toList()
}

class DurableGuideWindowPage(
    programmes: List<DurableGuideProgrammeRecord>,
    val nextCursor: DurableGuideCursor?,
    /** True when more overlapping rows exist beyond this bounded page. */
    val truncated: Boolean,
) {
    val programmes: List<DurableGuideProgrammeRecord> = programmes.toList()
}

data class DurableGuideNowNext(
    val current: DurableGuideProgrammeRecord?,
    val next: DurableGuideProgrammeRecord?,
)

sealed interface DurableGuideDeleteResult {
    class Deleted(val revision: Long) : DurableGuideDeleteResult {
        init { require(revision > 0) }
    }

    class Superseded(val current: DurableGuideSnapshot?) : DurableGuideDeleteResult
}

sealed interface DurableGuidePruneResult {
    class Published(val snapshot: DurableGuideSnapshot) : DurableGuidePruneResult
    class Unchanged(val current: DurableGuideSnapshot) : DurableGuidePruneResult
    class Superseded(val current: DurableGuideSnapshot?) : DurableGuidePruneResult
}

/** Backend-owned CAS token representing the complete active feed set for one source. */
interface DurableGuideSourceToken

class DurableGuideSourceSnapshot(
    val sourceKey: DurableGuideSourceKey,
    /** Number of currently published feeds; staged-only feeds are intentionally excluded. */
    val feedCount: Int,
    val token: DurableGuideSourceToken,
) {
    init { require(feedCount >= 0) }
    override fun toString(): String = "DurableGuideSourceSnapshot(feedCount=$feedCount, token=<redacted>)"
}

sealed interface DurableGuideSourceDeleteResult {
    class Deleted(
        val activeFeedCount: Int,
        val stagedOnlyFeedCount: Int,
    ) : DurableGuideSourceDeleteResult {
        init { require(activeFeedCount >= 0 && stagedOnlyFeedCount >= 0) }
    }

    class Superseded(
        val currentActiveFeedCount: Int,
        val currentStagedOnlyFeedCount: Int,
    ) : DurableGuideSourceDeleteResult {
        init { require(currentActiveFeedCount >= 0 && currentStagedOnlyFeedCount >= 0) }
    }
}

class DurableGuideMultiChannelWindowRequest(
    channelKeys: List<DurableGuideChannelKey>,
    val from: Instant,
    val until: Instant,
    val perChannelLimit: Int = DurableGuideLimits.MAX_WINDOW_ITEMS,
    val totalLimit: Int = DurableGuideLimits.MAX_MULTI_WINDOW_ITEMS,
    val payloadByteLimit: Int = DurableGuideLimits.MAX_MULTI_WINDOW_PAYLOAD_BYTES,
) {
    val channelKeys: List<DurableGuideChannelKey> = channelKeys.toList()

    init {
        require(this.channelKeys.isNotEmpty() && this.channelKeys.size <= DurableGuideLimits.MAX_MULTI_WINDOW_CHANNELS)
        require(this.channelKeys.distinct().size == this.channelKeys.size) { "Guide channel keys must be unique" }
        require(from < until)
        require(perChannelLimit in 1..DurableGuideLimits.MAX_WINDOW_ITEMS)
        require(totalLimit in 1..DurableGuideLimits.MAX_MULTI_WINDOW_ITEMS)
        require(payloadByteLimit in 1..DurableGuideLimits.MAX_MULTI_WINDOW_PAYLOAD_BYTES)
    }
}

class DurableGuideChannelWindow(
    val channelKey: DurableGuideChannelKey,
    programmes: List<DurableGuideProgrammeRecord>,
    val truncated: Boolean,
) {
    val programmes: List<DurableGuideProgrammeRecord> = programmes.toList()

    init { require(this.programmes.size <= DurableGuideLimits.MAX_WINDOW_ITEMS) }
}

/** Requested-order logical snapshot-bound result; backends may use bounded internal transactions. */
class DurableGuideMultiChannelWindow(
    channels: List<DurableGuideChannelWindow>,
    val payloadBytes: Int,
    val truncated: Boolean,
) {
    val channels: List<DurableGuideChannelWindow> = channels.toList()

    init {
        require(this.channels.size <= DurableGuideLimits.MAX_MULTI_WINDOW_CHANNELS)
        require(
            this.channels.sumOf { it.programmes.size.toLong() } <=
                DurableGuideLimits.MAX_MULTI_WINDOW_ITEMS.toLong(),
        )
        require(payloadBytes in 0..DurableGuideLimits.MAX_MULTI_WINDOW_PAYLOAD_BYTES)
    }
}

/** Stable logical encoded-payload accounting shared by all durable backends. */
object DurableGuidePayloadSizing {
    fun programmeBytes(programme: DurableGuideProgrammeRecord): Int =
        16 +
            programme.channelKey.value.encodedFieldBytes() +
            programme.winnerKey.value.encodedFieldBytes() +
            programme.title.encodedFieldBytes() +
            programme.subtitle.encodedFieldBytes() +
            programme.description.encodedFieldBytes() +
            programme.episode.encodedFieldBytes() +
            programme.artworkReference.encodedFieldBytes() +
            4 + programme.categories.sumOf { it.encodedFieldBytes() }
}

data class DurableGuideCleanupResult(val removedRows: Int, val hasMore: Boolean) {
    init { require(removedRows >= 0) }
}

/**
 * Feed-scoped durable guide capability.
 *
 * Pages are read only through a snapshot lease, so a cold projection can page a
 * fixed generation while a newer refresh publishes. Release leases in a
 * cancellation-safe `finally` block. Implementations retain revision tombstones
 * across deletion and cleanup, preventing stale generations after restart. Every
 * lease/cursor read throws [DurableGuideStoreException.Stale] for expired, foreign,
 * or wrong-operation tokens; [programme] and [acquire] deliberately fail closed with null.
 */
interface DurableGuideStore {
    val supported: Boolean
        get() = true

    /**
     * Renewing before this bounded idle timeout keeps a pinned generation readable.
     * Expired leases are stale and must never prevent cleanup.
     */
    val leaseIdleTimeoutMillis: Long

    /** Live writers must renew or stage before this timeout; expired staging is cleanup-eligible. */
    val generationIdleTimeoutMillis: Long

    /** Supersedes the previous writer for [key]; cleanup may reap it but never this live token. */
    suspend fun beginRefresh(key: DurableGuideKey, retention: DurableGuideRetention): DurableGuideGeneration

    suspend fun renewGeneration(generation: DurableGuideGeneration): Boolean

    /**
     * Idempotent writer cleanup. Call from a `NonCancellable` `finally` block when the owning
     * coroutine is cancelled. False means this owned token was already terminal.
     */
    suspend fun abandon(generation: DurableGuideGeneration): Boolean

    /**
     * Stages at most [DurableGuideLimits.MAX_BATCH_ITEMS] total channel plus programme rows and
     * returns generation-wide unique counts after deterministic dedupe. Implementations persist
     * attempted batch/channel/programme counters before dedupe; renewals never reset those caps.
     */
    suspend fun stage(
        generation: DurableGuideGeneration,
        channels: List<DurableGuideChannelRecord> = emptyList(),
        programmes: List<DurableGuideProgrammeRecord> = emptyList(),
    ): DurableGuideCounts

    /** Empty or over-cap generations throw [DurableGuideStoreException.Limit] without replacing active data. */
    suspend fun activate(
        generation: DurableGuideGeneration,
        expected: DurableGuideCounts,
    ): DurableGuideActivation

    suspend fun snapshot(key: DurableGuideKey): DurableGuideSnapshot?

    /** Bounded feed enumeration pinned to [source]'s mutation token for startup restoration. */
    suspend fun snapshots(
        source: DurableGuideSourceSnapshot,
        after: DurableGuideCursor? = null,
        limit: Int,
    ): DurableGuideSnapshotPage

    /** Returns null for a foreign/stale snapshot; throws [DurableGuideStoreException.Limit] at the live-lease cap. */
    suspend fun acquire(snapshot: DurableGuideSnapshot): DurableGuideSnapshotLease?
    suspend fun renew(lease: DurableGuideSnapshotLease): Boolean
    suspend fun release(lease: DurableGuideSnapshotLease)

    /** Invalid/expired leases and foreign or wrong-operation cursors throw [DurableGuideStoreException.Stale]. */
    suspend fun channels(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor? = null,
        limit: Int,
    ): DurableGuideChannelPage

    suspend fun programmeSearchRows(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor? = null,
        limit: Int,
    ): DurableGuideProgrammeSearchPage

    /** Returns null for a locator from another backend, feed, generation, revision, or expired lease. */
    suspend fun programme(
        lease: DurableGuideSnapshotLease,
        locator: DurableGuideProgrammeLocator,
    ): DurableGuideProgrammeRecord?

    suspend fun window(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        after: DurableGuideCursor? = null,
        limit: Int,
    ): DurableGuideWindowPage

    suspend fun nowNext(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        at: Instant,
    ): DurableGuideNowNext

    /**
     * Requested-order, empty-channel-preserving logical snapshot call. A backend may use multiple
     * bounded internal pages/transactions while the lease preserves one revision.
     */
    suspend fun multiChannelWindow(
        lease: DurableGuideSnapshotLease,
        request: DurableGuideMultiChannelWindowRequest,
    ): DurableGuideMultiChannelWindow

    /**
     * Republishes under subset bounds. Anchor changes or widening throw `Limit`; removing no rows
     * returns `Unchanged` without incrementing the active content revision.
     */
    suspend fun prune(
        key: DurableGuideKey,
        expectedRevision: Long,
        expectedMutationEpoch: Long,
        retention: DurableGuideRetention,
    ): DurableGuidePruneResult

    suspend fun deleteGuide(
        key: DurableGuideKey,
        expectedRevision: Long? = null,
        expectedMutationEpoch: Long? = null,
    ): DurableGuideDeleteResult

    suspend fun sourceSnapshot(sourceKey: DurableGuideSourceKey): DurableGuideSourceSnapshot

    /** [expected] provides bounded CAS without materializing an unbounded feed/revision map. */
    suspend fun deleteSource(
        sourceKey: DurableGuideSourceKey,
        expected: DurableGuideSourceToken? = null,
    ): DurableGuideSourceDeleteResult

    /** Never removes a live writer or live snapshot lease; work is strictly row-bounded. */
    suspend fun cleanupUnreachable(
        maxRows: Int = DurableGuideLimits.DEFAULT_CLEANUP_ROWS,
    ): DurableGuideCleanupResult
}

/** Explicit capability returned by stores that have not migrated guide persistence. */
object UnsupportedDurableGuideStore : DurableGuideStore {
    override val supported: Boolean = false
    override val leaseIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_LEASE_IDLE_TIMEOUT_MILLIS
    override val generationIdleTimeoutMillis: Long = DurableGuideLimits.DEFAULT_GENERATION_IDLE_TIMEOUT_MILLIS

    private fun unsupported(): Nothing = throw DurableGuideStoreException.Unsupported()

    override suspend fun beginRefresh(key: DurableGuideKey, retention: DurableGuideRetention) = unsupported()
    override suspend fun renewGeneration(generation: DurableGuideGeneration) = unsupported()
    override suspend fun abandon(generation: DurableGuideGeneration) = unsupported()
    override suspend fun stage(
        generation: DurableGuideGeneration,
        channels: List<DurableGuideChannelRecord>,
        programmes: List<DurableGuideProgrammeRecord>,
    ) = unsupported()
    override suspend fun activate(generation: DurableGuideGeneration, expected: DurableGuideCounts) = unsupported()
    override suspend fun snapshot(key: DurableGuideKey) = unsupported()
    override suspend fun snapshots(source: DurableGuideSourceSnapshot, after: DurableGuideCursor?, limit: Int) = unsupported()
    override suspend fun acquire(snapshot: DurableGuideSnapshot) = unsupported()
    override suspend fun renew(lease: DurableGuideSnapshotLease) = unsupported()
    override suspend fun release(lease: DurableGuideSnapshotLease) = unsupported()
    override suspend fun channels(lease: DurableGuideSnapshotLease, after: DurableGuideCursor?, limit: Int) = unsupported()
    override suspend fun programmeSearchRows(
        lease: DurableGuideSnapshotLease,
        after: DurableGuideCursor?,
        limit: Int,
    ) = unsupported()
    override suspend fun programme(lease: DurableGuideSnapshotLease, locator: DurableGuideProgrammeLocator) = unsupported()
    override suspend fun window(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        from: Instant,
        until: Instant,
        after: DurableGuideCursor?,
        limit: Int,
    ) = unsupported()
    override suspend fun nowNext(
        lease: DurableGuideSnapshotLease,
        channelKey: DurableGuideChannelKey,
        at: Instant,
    ) = unsupported()
    override suspend fun multiChannelWindow(
        lease: DurableGuideSnapshotLease,
        request: DurableGuideMultiChannelWindowRequest,
    ) = unsupported()
    override suspend fun prune(
        key: DurableGuideKey,
        expectedRevision: Long,
        expectedMutationEpoch: Long,
        retention: DurableGuideRetention,
    ) = unsupported()
    override suspend fun deleteGuide(
        key: DurableGuideKey,
        expectedRevision: Long?,
        expectedMutationEpoch: Long?,
    ) = unsupported()
    override suspend fun sourceSnapshot(sourceKey: DurableGuideSourceKey) = unsupported()
    override suspend fun deleteSource(sourceKey: DurableGuideSourceKey, expected: DurableGuideSourceToken?) = unsupported()
    override suspend fun cleanupUnreachable(maxRows: Int) = unsupported()

    override fun toString(): String = "UnsupportedDurableGuideStore"
}

sealed class DurableGuideStoreException(message: String) : IllegalStateException(message) {
    class Unsupported : DurableGuideStoreException("Durable guide storage is unsupported")
    class Stale : DurableGuideStoreException("Durable guide token is stale")
    class Limit : DurableGuideStoreException("Durable guide operation exceeds a configured limit")
    class Corrupt : DurableGuideStoreException("Durable guide storage is corrupt")
}

private fun requireOpaque(value: String, maxChars: Int, label: String) {
    require(value.isNotBlank() && value.length <= maxChars) { "$label has an invalid length" }
    require(value.all { it.isLetterOrDigit() || it == '-' || it == '_' || it == '.' }) {
        "$label must be an app-generated opaque key"
    }
}

private fun requireDigest(value: String, label: String) {
    require(value.length == DurableGuideLimits.OPAQUE_DIGEST_CHARS && value.all { it in '0'..'9' || it in 'a'..'f' }) {
        "$label must be a lowercase SHA-256 digest"
    }
}

private fun StringBuilder.appendCanonicalField(value: String?) {
    if (value == null) {
        append("-1:;")
    } else {
        append(value.encodeToByteArray().size).append(':').append(value).append(';')
    }
}

private fun String?.encodedFieldBytes(): Int = 16 + (this?.encodeToByteArray()?.size ?: 0)

internal fun sha256Hex(input: ByteArray): String {
    val padding = (64 + 56 - ((input.size + 1) % 64)) % 64
    val message = ByteArray(input.size + 1 + padding + 8)
    input.copyInto(message)
    message[input.size] = 0x80.toByte()
    val bitLength = input.size.toLong() * 8
    repeat(8) { index ->
        message[message.lastIndex - index] = (bitLength ushr (index * 8)).toByte()
    }

    val hash = SHA256_INITIAL.copyOf()
    val words = IntArray(64)
    var offset = 0
    while (offset < message.size) {
        repeat(16) { index ->
            val start = offset + index * 4
            words[index] =
                ((message[start].toInt() and 0xff) shl 24) or
                ((message[start + 1].toInt() and 0xff) shl 16) or
                ((message[start + 2].toInt() and 0xff) shl 8) or
                (message[start + 3].toInt() and 0xff)
        }
        for (index in 16 until 64) {
            val s0 = words[index - 15].rotateRight(7) xor
                words[index - 15].rotateRight(18) xor (words[index - 15] ushr 3)
            val s1 = words[index - 2].rotateRight(17) xor
                words[index - 2].rotateRight(19) xor (words[index - 2] ushr 10)
            words[index] = words[index - 16] + s0 + words[index - 7] + s1
        }

        var a = hash[0]
        var b = hash[1]
        var c = hash[2]
        var d = hash[3]
        var e = hash[4]
        var f = hash[5]
        var g = hash[6]
        var h = hash[7]
        repeat(64) { index ->
            val sum1 = e.rotateRight(6) xor e.rotateRight(11) xor e.rotateRight(25)
            val choice = (e and f) xor (e.inv() and g)
            val first = h + sum1 + choice + SHA256_ROUND[index] + words[index]
            val sum0 = a.rotateRight(2) xor a.rotateRight(13) xor a.rotateRight(22)
            val majority = (a and b) xor (a and c) xor (b and c)
            val second = sum0 + majority
            h = g
            g = f
            f = e
            e = d + first
            d = c
            c = b
            b = a
            a = first + second
        }
        hash[0] += a
        hash[1] += b
        hash[2] += c
        hash[3] += d
        hash[4] += e
        hash[5] += f
        hash[6] += g
        hash[7] += h
        offset += 64
    }
    return hash.joinToString("") { value ->
        (value.toLong() and 0xffff_ffffL).toString(16).padStart(8, '0')
    }
}

private val SHA256_INITIAL: IntArray = longArrayOf(
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
).map(Long::toInt).toIntArray()

private val SHA256_ROUND: IntArray = longArrayOf(
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
).map(Long::toInt).toIntArray()

private fun String.requireSafeArtworkReference() {
    val value = trim()
    require(value.isNotEmpty() && value.length <= DurableGuideLimits.MAX_ARTWORK_REFERENCE_CHARS) {
        "Artwork reference is invalid"
    }
    val lower = value.lowercase()
    require(lower.startsWith("https://") || lower.startsWith("http://")) {
        "Artwork reference must use HTTP or HTTPS"
    }
    require(!lower.startsWith("data:") && !lower.startsWith("blob:")) {
        "Inline artwork references are not accepted"
    }
    val schemeEnd = lower.indexOf("://")
    if (schemeEnd >= 0) {
        val authorityEnd = lower.indexOfAny(charArrayOf('/', '?', '#'), schemeEnd + 3)
            .let { if (it == -1) lower.length else it }
        val authority = lower.substring(schemeEnd + 3, authorityEnd)
        require(authority.isNotEmpty()) { "Artwork reference authority is required" }
        require('@' !in authority) {
            "Credential-bearing artwork references are not accepted"
        }
    }
    require('?' !in lower) { "Query-bearing artwork references are not accepted" }
    require('#' !in lower) { "Fragment-bearing artwork references are not accepted" }
}
