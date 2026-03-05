package com.initializedmodel.liverunner

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import androidx.core.content.ContextCompat

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        val action = intent?.action.orEmpty()
        if (
            action != Intent.ACTION_BOOT_COMPLETED &&
            action != Intent.ACTION_MY_PACKAGE_REPLACED
        ) {
            return
        }

        val startIntent = Intent(context, LiveRunnerService::class.java).apply {
            this.action = LiveRunnerService.ACTION_START
        }
        ContextCompat.startForegroundService(context, startIntent)
    }
}
