package com.initializedmodel.liverunner

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.util.concurrent.TimeUnit

class LiveApiClient {
    data class ApiResult(
        val ok: Boolean,
        val code: Int,
        val body: String,
    )

    private val client = OkHttpClient.Builder()
        .callTimeout(10, TimeUnit.SECONDS)
        .build()

    fun checkHealth(baseUrl: String): Boolean {
        val url = "${baseUrl.trim().trimEnd('/')}/healthz"
        val req = Request.Builder().url(url).get().build()
        client.newCall(req).execute().use { resp ->
            return resp.isSuccessful
        }
    }

    fun statusCheck(
        baseUrl: String,
        token: String,
        robotId: String,
        apiKey: String,
        apiSecret: String,
    ): ApiResult {
        val url = "${baseUrl.trim().trimEnd('/')}/api/live/robots/${robotId.trim()}/status-check"
        val payload = JSONObject()
        if (apiKey.isNotBlank() && apiSecret.isNotBlank()) {
            payload.put("api_key", apiKey.trim())
            payload.put("api_secret", apiSecret.trim())
        }
        val reqBuilder = Request.Builder()
            .url(url)
            .post(payload.toString().toRequestBody("application/json; charset=utf-8".toMediaType()))
            .addHeader("Content-Type", "application/json")

        if (token.isNotBlank()) {
            reqBuilder.addHeader("Authorization", "Bearer ${token.trim()}")
        }

        client.newCall(reqBuilder.build()).execute().use { resp ->
            val body = resp.body?.string().orEmpty()
            return ApiResult(ok = resp.isSuccessful, code = resp.code, body = body)
        }
    }
}
