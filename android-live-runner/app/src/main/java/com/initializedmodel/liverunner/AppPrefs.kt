package com.initializedmodel.liverunner

import android.content.Context

object AppPrefs {
    private const val PREF_NAME = "live_runner_prefs"
    private const val KEY_BASE_URL = "base_url"
    private const val KEY_TOKEN = "token"
    private const val KEY_ROBOT_ID = "robot_id"
    private const val KEY_POLL_SECONDS = "poll_seconds"
    private const val KEY_TERMUX_SCRIPT = "termux_script"

    data class RunnerConfig(
        val baseUrl: String,
        val token: String,
        val robotId: String,
        val pollSeconds: Int,
        val termuxScriptPath: String,
    )

    fun load(context: Context): RunnerConfig {
        val pref = context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)
        return RunnerConfig(
            baseUrl = pref.getString(KEY_BASE_URL, "http://127.0.0.1:8010").orEmpty(),
            token = pref.getString(KEY_TOKEN, "").orEmpty(),
            robotId = pref.getString(KEY_ROBOT_ID, "").orEmpty(),
            pollSeconds = pref.getInt(KEY_POLL_SECONDS, 8),
            termuxScriptPath = pref.getString(
                KEY_TERMUX_SCRIPT,
                "/data/data/com.termux/files/home/initializedmodel_2/deploy/android/termux/start_live_api_tmux.sh",
            ).orEmpty(),
        )
    }

    fun save(context: Context, config: RunnerConfig) {
        val pref = context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)
        pref.edit()
            .putString(KEY_BASE_URL, config.baseUrl.trim())
            .putString(KEY_TOKEN, config.token.trim())
            .putString(KEY_ROBOT_ID, config.robotId.trim())
            .putInt(KEY_POLL_SECONDS, config.pollSeconds)
            .putString(KEY_TERMUX_SCRIPT, config.termuxScriptPath.trim())
            .apply()
    }
}
