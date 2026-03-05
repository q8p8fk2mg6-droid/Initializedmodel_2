package com.initializedmodel.liverunner

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

class LiveRunnerService : Service() {
    companion object {
        const val ACTION_START = "com.initializedmodel.liverunner.action.START"
        const val ACTION_STOP = "com.initializedmodel.liverunner.action.STOP"
        private const val CHANNEL_ID = "live_runner_channel"
        private const val NOTIFICATION_ID = 2001
    }

    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loopJob: Job? = null
    private val apiClient = LiveApiClient()
    private lateinit var secureStore: SecureStore

    override fun onCreate() {
        super.onCreate()
        secureStore = SecureStore(this)
        ensureNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> {
                stopLoop()
                stopSelf()
                return START_NOT_STICKY
            }

            else -> startLoop()
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?) = null

    override fun onDestroy() {
        stopLoop()
        super.onDestroy()
    }

    private fun startLoop() {
        if (loopJob?.isActive == true) return

        startForeground(NOTIFICATION_ID, buildNotification("Starting live runner..."))
        loopJob = serviceScope.launch {
            var healthFailures = 0
            while (isActive) {
                val cfg = AppPrefs.load(this@LiveRunnerService)
                val pollSeconds = cfg.pollSeconds.coerceIn(5, 10)
                val creds = secureStore.loadExchangeCredentials()

                val healthy = runCatching { apiClient.checkHealth(cfg.baseUrl) }.getOrDefault(false)
                if (!healthy) {
                    healthFailures += 1
                    val restarted = if (healthFailures >= 3) {
                        healthFailures = 0
                        TermuxBridge.startScript(this@LiveRunnerService, cfg.termuxScriptPath)
                    } else {
                        false
                    }
                    val msg = if (restarted) {
                        "Health failed, triggered Termux restart."
                    } else {
                        "Health failed, retrying..."
                    }
                    updateNotification(msg)
                    delay(pollSeconds * 1000L)
                    continue
                }

                healthFailures = 0

                if (cfg.robotId.isBlank()) {
                    updateNotification("Runner alive. Waiting for robot_id.")
                    delay(pollSeconds * 1000L)
                    continue
                }

                val result = runCatching {
                    apiClient.statusCheck(
                        baseUrl = cfg.baseUrl,
                        token = cfg.token,
                        robotId = cfg.robotId,
                        apiKey = creds.apiKey,
                        apiSecret = creds.apiSecret,
                    )
                }.getOrElse { err ->
                    LiveApiClient.ApiResult(
                        ok = false,
                        code = 0,
                        body = err.message.orEmpty(),
                    )
                }

                if (result.ok) {
                    updateNotification("status-check ok (robot=${cfg.robotId}).")
                } else {
                    updateNotification("status-check failed (${result.code}): ${result.body.take(120)}")
                }
                delay(pollSeconds * 1000L)
            }
        }
    }

    private fun stopLoop() {
        loopJob?.cancel()
        loopJob = null
        stopForeground(STOP_FOREGROUND_REMOVE)
    }

    private fun ensureNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val manager = getSystemService(NotificationManager::class.java)
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Live Runner",
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = "Foreground live robot status polling"
        }
        manager.createNotificationChannel(channel)
    }

    private fun buildNotification(text: String): android.app.Notification {
        val openIntent = Intent(this, MainActivity::class.java)
        val pending = PendingIntent.getActivity(
            this,
            0,
            openIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_stat_live)
            .setContentTitle("Android Live Runner")
            .setContentText(text)
            .setContentIntent(pending)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        val manager = getSystemService(NotificationManager::class.java)
        manager.notify(NOTIFICATION_ID, buildNotification(text))
    }
}
