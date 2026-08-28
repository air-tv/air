@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package com.getair.core.source

import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import platform.CoreFoundation.CFDictionaryAddValue
import platform.CoreFoundation.CFDictionaryCreateMutable
import platform.CoreFoundation.CFDictionaryRef
import platform.CoreFoundation.CFRelease
import platform.CoreFoundation.CFTypeRef
import platform.CoreFoundation.CFTypeRefVar
import platform.CoreFoundation.kCFBooleanTrue
import platform.Foundation.CFBridgingRelease
import platform.Foundation.CFBridgingRetain
import platform.Foundation.NSData
import platform.Foundation.NSString
import platform.Foundation.NSUTF8StringEncoding
import platform.Foundation.create
import platform.Foundation.dataUsingEncoding
import platform.Security.SecItemAdd
import platform.Security.SecItemCopyMatching
import platform.Security.SecItemDelete
import platform.Security.SecItemUpdate
import platform.Security.errSecDuplicateItem
import platform.Security.errSecItemNotFound
import platform.Security.errSecSuccess
import platform.Security.kSecAttrAccessible
import platform.Security.kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
import platform.Security.kSecAttrAccount
import platform.Security.kSecAttrService
import platform.Security.kSecClass
import platform.Security.kSecClassGenericPassword
import platform.Security.kSecMatchLimit
import platform.Security.kSecMatchLimitOne
import platform.Security.kSecReturnData
import platform.Security.kSecValueData

/** Apple Keychain generic-password vault for iOS and native macOS shells. */
class AppleKeychainLocalSourceSecretStore(
    private val service: String = DEFAULT_SERVICE,
) : LocalSourceSecretStore {
    private val mutex = Mutex()

    init {
        require(service.isNotBlank() && service.length <= 255 && '\u0000' !in service)
    }

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock {
        withSourceBridged(service, id.accountName()) { values ->
            withSourceDictionary(
                kSecClass to kSecClassGenericPassword,
                kSecAttrService to values[0],
                kSecAttrAccount to values[1],
                kSecReturnData to kCFBooleanTrue,
                kSecMatchLimit to kSecMatchLimitOne,
            ) { query ->
                memScoped {
                    val result = alloc<CFTypeRefVar>()
                    result.value = null
                    when (SecItemCopyMatching(query, result.ptr)) {
                        errSecItemNotFound -> null
                        errSecSuccess -> {
                            val data = CFBridgingRelease(result.value) as? NSData
                                ?: throw IllegalStateException("Apple Keychain returned invalid source credential data")
                            val encoded = NSString.create(data, NSUTF8StringEncoding) as String?
                                ?: throw IllegalStateException("Apple Keychain source credential is not UTF-8")
                            decodeLocalSourceSecret(encoded)
                        }
                        else -> throw IllegalStateException("Apple Keychain source credential could not be read")
                    }
                }
            }
        }
    }

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        val encoded = secret.encodeForVault()
        val data = NSString.create(string = encoded).dataUsingEncoding(NSUTF8StringEncoding)
            ?: throw IllegalStateException("Source credential could not be encoded for Apple Keychain")
        mutex.withLock {
            withSourceBridged(service, id.accountName(), data) { values ->
                when (update(values)) {
                    errSecSuccess -> Unit
                    errSecItemNotFound -> add(values)
                    else -> throw IllegalStateException("Apple Keychain source credential could not be written")
                }
            }
        }
    }

    override suspend fun remove(id: LocalSourceId) {
        mutex.withLock {
            withSourceBridged(service, id.accountName()) { values ->
                withSourceDictionary(
                    kSecClass to kSecClassGenericPassword,
                    kSecAttrService to values[0],
                    kSecAttrAccount to values[1],
                ) { query ->
                    when (SecItemDelete(query)) {
                        errSecSuccess, errSecItemNotFound -> Unit
                        else -> throw IllegalStateException("Apple Keychain source credential could not be removed")
                    }
                }
            }
        }
    }

    private fun update(values: List<CFTypeRef?>): Int = withSourceDictionary(
        kSecClass to kSecClassGenericPassword,
        kSecAttrService to values[0],
        kSecAttrAccount to values[1],
    ) { query ->
        withSourceDictionary(kSecValueData to values[2]) { changes -> SecItemUpdate(query, changes) }
    }

    private fun add(values: List<CFTypeRef?>) {
        val status = withSourceDictionary(
            kSecClass to kSecClassGenericPassword,
            kSecAttrService to values[0],
            kSecAttrAccount to values[1],
            kSecAttrAccessible to kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            kSecValueData to values[2],
        ) { attributes -> SecItemAdd(attributes, null) }
        if (status == errSecDuplicateItem && update(values) == errSecSuccess) return
        if (status != errSecSuccess) {
            throw IllegalStateException("Apple Keychain source credential could not be written")
        }
    }

    override fun toString(): String = "AppleKeychainLocalSourceSecretStore(service=<redacted>)"

    private companion object {
        const val DEFAULT_SERVICE = "com.getair.sources.credentials.v1"
    }
}

private inline fun <T> withSourceBridged(
    vararg objects: Any?,
    block: (List<CFTypeRef?>) -> T,
): T {
    val references = objects.map { CFBridgingRetain(it) }
    return try {
        block(references)
    } finally {
        references.forEach { reference -> if (reference != null) CFBridgingRelease(reference) }
    }
}

private inline fun <T> withSourceDictionary(
    vararg values: Pair<CFTypeRef?, CFTypeRef?>,
    block: (CFDictionaryRef?) -> T,
): T {
    val dictionary = checkNotNull(CFDictionaryCreateMutable(null, values.size.convert(), null, null))
    values.forEach { (key, value) ->
        require(key != null && value != null)
        CFDictionaryAddValue(dictionary, key, value)
    }
    return try {
        block(dictionary)
    } finally {
        CFRelease(dictionary)
    }
}

private fun LocalSourceId.accountName(): String = "source:$value"
