package com.getair.core.persistence

import com.getair.core.history.ContinueWatchingState
import com.getair.core.history.LocalContinueWatchingStore
import com.getair.core.household.HouseholdState
import com.getair.core.household.LocalHouseholdStore
import com.getair.core.source.LocalSourceState
import com.getair.core.source.LocalSourceStore
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Small platform seam for non-secret application documents. Source credentials
 * must remain in `LocalSourceSecretStore`, never in this store.
 */
interface LocalDocumentStore {
    suspend fun read(document: String): String?
    suspend fun write(document: String, value: String)
    suspend fun remove(document: String)
}

/** Tests, previews, and explicitly ephemeral browser sessions. */
class InMemoryDocumentStore : LocalDocumentStore {
    private val mutex = Mutex()
    private val values = mutableMapOf<String, String>()

    override suspend fun read(document: String): String? = mutex.withLock {
        validateLocalDocumentName(document)
        values[document]
    }

    override suspend fun write(document: String, value: String) {
        validateLocalDocumentName(document)
        mutex.withLock { values[document] = value }
    }

    override suspend fun remove(document: String) {
        validateLocalDocumentName(document)
        mutex.withLock { values.remove(document) }
    }

    override fun toString(): String = "InMemoryDocumentStore(values=<redacted>)"
}

/** Shared bounded behavior behind the JS and Wasm localStorage factories. */
internal class BrowserDocumentStore(
    namespace: String,
    private val readValue: (String) -> String?,
    private val writeValue: (String, String) -> Unit,
    private val removeValue: (String) -> Unit,
    private val maximumDocumentChars: Int = 4 * 1024 * 1024,
) : LocalDocumentStore {
    private val mutex = Mutex()
    private val namespace = namespace.also(::validateLocalDocumentName)

    init {
        require(maximumDocumentChars > 0)
    }

    override suspend fun read(document: String): String? = mutex.withLock {
        validateLocalDocumentName(document)
        readValue(key(document))?.also {
            require(it.length <= maximumDocumentChars) { "Local application document is too large" }
        }
    }

    override suspend fun write(document: String, value: String) {
        validateLocalDocumentName(document)
        require(value.length <= maximumDocumentChars) { "Local application document is too large" }
        mutex.withLock { writeValue(key(document), value) }
    }

    override suspend fun remove(document: String) {
        validateLocalDocumentName(document)
        mutex.withLock { removeValue(key(document)) }
    }

    private fun key(document: String): String = "$namespace:$document"

    override fun toString(): String = "BrowserDocumentStore(namespace=<redacted>)"
}

class PersistentHouseholdStore private constructor(
    private val delegate: PersistentDocumentState<HouseholdState>,
) : LocalHouseholdStore {
    override val state: StateFlow<HouseholdState> = delegate.state
    override suspend fun replace(state: HouseholdState) = delegate.replace(state)

    companion object {
        suspend fun open(
            documents: LocalDocumentStore,
            document: String = "household.v1",
            initial: HouseholdState = HouseholdState(),
        ): PersistentHouseholdStore = PersistentHouseholdStore(
            PersistentDocumentState.open(
                documents,
                document,
                initial,
                HouseholdState.serializer(),
                MAX_HOUSEHOLD_CHARS,
            ),
        )
    }
}

class PersistentLocalSourceStore private constructor(
    private val delegate: PersistentDocumentState<LocalSourceState>,
) : LocalSourceStore {
    override val state: StateFlow<LocalSourceState> = delegate.state
    override suspend fun replace(state: LocalSourceState) = delegate.replace(state)

    companion object {
        suspend fun open(
            documents: LocalDocumentStore,
            document: String = "sources.v1",
            initial: LocalSourceState = LocalSourceState(),
        ): PersistentLocalSourceStore = PersistentLocalSourceStore(
            PersistentDocumentState.open(
                documents,
                document,
                initial,
                LocalSourceState.serializer(),
                MAX_SOURCE_METADATA_CHARS,
            ),
        )
    }
}

class PersistentContinueWatchingStore private constructor(
    private val delegate: PersistentDocumentState<ContinueWatchingState>,
) : LocalContinueWatchingStore {
    override val state: StateFlow<ContinueWatchingState> = delegate.state
    override suspend fun replace(state: ContinueWatchingState) = delegate.replace(state)

    companion object {
        suspend fun open(
            documents: LocalDocumentStore,
            document: String = "continue-watching.v1",
            initial: ContinueWatchingState = ContinueWatchingState(),
        ): PersistentContinueWatchingStore = PersistentContinueWatchingStore(
            PersistentDocumentState.open(
                documents,
                document,
                initial,
                ContinueWatchingState.serializer(),
                MAX_CONTINUE_WATCHING_CHARS,
            ),
        )
    }
}

private class PersistentDocumentState<T> private constructor(
    private val documents: LocalDocumentStore,
    private val document: String,
    initial: T,
    private val serializer: KSerializer<T>,
    private val maximumChars: Int,
) {
    private val commands = Mutex()
    private val mutableState = MutableStateFlow(initial)
    val state: StateFlow<T> = mutableState.asStateFlow()

    suspend fun replace(value: T) = commands.withLock {
        if (value == mutableState.value) return@withLock
        val encoded = encodeDocument(serializer, value, maximumChars)
        documents.write(document, encoded)
        mutableState.value = value
    }

    companion object {
        suspend fun <T> open(
            documents: LocalDocumentStore,
            document: String,
            initial: T,
            serializer: KSerializer<T>,
            maximumChars: Int,
        ): PersistentDocumentState<T> {
            validateLocalDocumentName(document)
            require(maximumChars > 0)
            val value = documents.read(document)?.let {
                decodeDocument(serializer, it, maximumChars)
            } ?: initial
            return PersistentDocumentState(documents, document, value, serializer, maximumChars)
        }
    }
}

@Serializable
private data class StoredDocument<T>(
    val version: Int,
    val value: T,
)

private val documentJson = Json {
    encodeDefaults = true
    explicitNulls = false
    ignoreUnknownKeys = true
    classDiscriminator = "_type"
}

private fun <T> encodeDocument(serializer: KSerializer<T>, value: T, maximumChars: Int): String {
    val encoded = documentJson.encodeToString(
        StoredDocument.serializer(serializer),
        StoredDocument(DOCUMENT_VERSION, value),
    )
    require(encoded.length <= maximumChars) { "Local application document is too large" }
    return encoded
}

private fun <T> decodeDocument(serializer: KSerializer<T>, value: String, maximumChars: Int): T {
    require(value.length <= maximumChars) { "Local application document is too large" }
    return try {
        val stored = documentJson.decodeFromString(StoredDocument.serializer(serializer), value)
        require(stored.version == DOCUMENT_VERSION)
        stored.value
    } catch (_: Exception) {
        // Serialization failures may quote malformed local content. Keep the failure
        // recoverable by the shell without retaining the document as an exception cause.
        throw IllegalStateException("Local application document is invalid")
    }
}

internal fun validateLocalDocumentName(value: String) {
    require(DOCUMENT_NAME.matches(value)) { "Local document name is invalid" }
}

private const val DOCUMENT_VERSION = 1
private const val MAX_HOUSEHOLD_CHARS = 1024 * 1024
private const val MAX_SOURCE_METADATA_CHARS = 1024 * 1024
private const val MAX_CONTINUE_WATCHING_CHARS = 4 * 1024 * 1024
private val DOCUMENT_NAME = Regex("[a-z0-9][a-z0-9._-]{0,127}")
