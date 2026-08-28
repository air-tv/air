package com.getair.core.source

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.security.KeyStore
import java.security.MessageDigest
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/** Android Keystore AES-GCM vault; SharedPreferences contains ciphertext only. */
class AndroidKeystoreLocalSourceSecretStore(
    context: Context,
    storageName: String = DEFAULT_STORAGE_NAME,
    private val keyAlias: String = DEFAULT_KEY_ALIAS,
) : LocalSourceSecretStore {
    private val mutex = Mutex()
    private val preferences = context.applicationContext.getSharedPreferences(storageName, Context.MODE_PRIVATE)
    private var cachedKey: SecretKey? = null

    init {
        require(storageName.isNotBlank())
        require(keyAlias.isNotBlank())
    }

    override suspend fun read(id: LocalSourceId): LocalSourceSecret? = mutex.withLock {
        withContext(Dispatchers.IO) {
            val encoded = preferences.getString(id.storageKey(), null) ?: return@withContext null
            decodeLocalSourceSecret(decrypt(encoded))
        }
    }

    override suspend fun write(id: LocalSourceId, secret: LocalSourceSecret) {
        val payload = secret.encodeForVault()
        mutex.withLock {
            withContext(Dispatchers.IO) {
                check(preferences.edit().putString(id.storageKey(), encrypt(payload)).commit()) {
                    "Android source credential ciphertext could not be committed"
                }
            }
        }
    }

    override suspend fun remove(id: LocalSourceId) {
        mutex.withLock {
            withContext(Dispatchers.IO) {
                check(preferences.edit().remove(id.storageKey()).commit()) {
                    "Android source credential ciphertext could not be removed"
                }
            }
        }
    }

    private fun encrypt(value: String): String {
        val cipher = Cipher.getInstance(CIPHER_TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, key())
        val iv = cipher.iv
        require(iv.size in 1..255)
        val ciphertext = cipher.doFinal(value.encodeToByteArray())
        val envelope = byteArrayOf(ENVELOPE_VERSION, iv.size.toByte()) + iv + ciphertext
        return Base64.encodeToString(envelope, Base64.NO_WRAP)
    }

    private fun decrypt(encoded: String): String {
        val envelope = Base64.decode(encoded, Base64.NO_WRAP)
        require(envelope.size >= 3 && envelope[0] == ENVELOPE_VERSION) {
            "Android source credential ciphertext has an unsupported envelope"
        }
        val ivLength = envelope[1].toInt() and 0xff
        require(ivLength > 0 && envelope.size > 2 + ivLength) {
            "Android source credential ciphertext is truncated"
        }
        val cipher = Cipher.getInstance(CIPHER_TRANSFORMATION)
        cipher.init(
            Cipher.DECRYPT_MODE,
            key(),
            GCMParameterSpec(GCM_TAG_BITS, envelope.copyOfRange(2, 2 + ivLength)),
        )
        return cipher.doFinal(envelope.copyOfRange(2 + ivLength, envelope.size)).decodeToString()
    }

    private fun key(): SecretKey = cachedKey ?: loadOrCreateKey().also { cachedKey = it }

    private fun loadOrCreateKey(): SecretKey {
        val keyStore = KeyStore.getInstance(ANDROID_KEYSTORE).apply { load(null) }
        (keyStore.getKey(keyAlias, null) as? SecretKey)?.let { return it }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, ANDROID_KEYSTORE).run {
            init(
                KeyGenParameterSpec.Builder(
                    keyAlias,
                    KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
                )
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                    .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .setRandomizedEncryptionRequired(true)
                    .build(),
            )
            generateKey()
        }
    }

    override fun toString(): String =
        "AndroidKeystoreLocalSourceSecretStore(storage=<redacted>, keyAlias=<redacted>)"

    private companion object {
        const val DEFAULT_STORAGE_NAME = "com.getair.sources.credentials"
        const val DEFAULT_KEY_ALIAS = "com.getair.sources.credentials.v1"
        const val ANDROID_KEYSTORE = "AndroidKeyStore"
        const val CIPHER_TRANSFORMATION = "AES/GCM/NoPadding"
        const val GCM_TAG_BITS = 128
        const val ENVELOPE_VERSION: Byte = 1
    }
}

private fun LocalSourceId.storageKey(): String = MessageDigest.getInstance("SHA-256")
    .digest(value.encodeToByteArray())
    .joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
