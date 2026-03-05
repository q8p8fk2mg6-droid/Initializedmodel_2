package com.initializedmodel.liverunner

import android.content.Context
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey

class SecureStore(context: Context) {
    private val masterKey = MasterKey.Builder(context)
        .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
        .build()

    private val prefs = EncryptedSharedPreferences.create(
        context,
        "live_runner_secure_prefs",
        masterKey,
        EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
        EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
    )

    data class ExchangeCredentials(
        val apiKey: String,
        val apiSecret: String,
    )

    fun saveExchangeCredentials(apiKey: String, apiSecret: String) {
        prefs.edit()
            .putString("exchange_api_key", apiKey.trim())
            .putString("exchange_api_secret", apiSecret.trim())
            .apply()
    }

    fun loadExchangeCredentials(): ExchangeCredentials {
        return ExchangeCredentials(
            apiKey = prefs.getString("exchange_api_key", "").orEmpty(),
            apiSecret = prefs.getString("exchange_api_secret", "").orEmpty(),
        )
    }
}
