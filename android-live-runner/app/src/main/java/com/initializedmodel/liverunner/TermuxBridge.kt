package com.initializedmodel.liverunner

import android.content.Context
import android.content.Intent

object TermuxBridge {
    private const val ACTION_RUN_COMMAND = "com.termux.app.RUN_COMMAND"
    private const val EXTRA_PATH = "com.termux.RUN_COMMAND_PATH"
    private const val EXTRA_WORKDIR = "com.termux.RUN_COMMAND_WORKDIR"
    private const val EXTRA_BACKGROUND = "com.termux.RUN_COMMAND_BACKGROUND"
    private const val EXTRA_ARGS = "com.termux.RUN_COMMAND_ARGUMENTS"

    fun startScript(context: Context, scriptPath: String): Boolean {
        val path = scriptPath.trim()
        if (path.isEmpty()) return false

        return try {
            val intent = Intent(ACTION_RUN_COMMAND).apply {
                setClassName("com.termux", "com.termux.app.RunCommandService")
                putExtra(EXTRA_PATH, path)
                putExtra(EXTRA_WORKDIR, "/data/data/com.termux/files/home")
                putExtra(EXTRA_BACKGROUND, true)
                putExtra(EXTRA_ARGS, arrayOf())
            }
            context.startService(intent)
            true
        } catch (_: Exception) {
            false
        }
    }
}
