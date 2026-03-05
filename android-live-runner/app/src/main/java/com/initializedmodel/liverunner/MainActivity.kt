package com.initializedmodel.liverunner

import android.content.Intent
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat

class MainActivity : AppCompatActivity() {
    private lateinit var secureStore: SecureStore

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        secureStore = SecureStore(this)

        val etBaseUrl = findViewById<EditText>(R.id.etBaseUrl)
        val etToken = findViewById<EditText>(R.id.etToken)
        val etRobotId = findViewById<EditText>(R.id.etRobotId)
        val etPollSeconds = findViewById<EditText>(R.id.etPollSeconds)
        val etTermuxScript = findViewById<EditText>(R.id.etTermuxScript)
        val etApiKey = findViewById<EditText>(R.id.etApiKey)
        val etApiSecret = findViewById<EditText>(R.id.etApiSecret)
        val tvStatus = findViewById<TextView>(R.id.tvStatus)

        fun loadConfig() {
            val cfg = AppPrefs.load(this)
            val creds = secureStore.loadExchangeCredentials()
            etBaseUrl.setText(cfg.baseUrl)
            etToken.setText(cfg.token)
            etRobotId.setText(cfg.robotId)
            etPollSeconds.setText(cfg.pollSeconds.toString())
            etTermuxScript.setText(cfg.termuxScriptPath)
            etApiKey.setText(creds.apiKey)
            etApiSecret.setText(creds.apiSecret)
        }

        findViewById<Button>(R.id.btnSave).setOnClickListener {
            val pollValue = etPollSeconds.text.toString().trim().toIntOrNull() ?: 8
            val cfg = AppPrefs.RunnerConfig(
                baseUrl = etBaseUrl.text.toString().trim(),
                token = etToken.text.toString().trim(),
                robotId = etRobotId.text.toString().trim(),
                pollSeconds = pollValue.coerceIn(5, 10),
                termuxScriptPath = etTermuxScript.text.toString().trim(),
            )
            AppPrefs.save(this, cfg)
            secureStore.saveExchangeCredentials(
                apiKey = etApiKey.text.toString(),
                apiSecret = etApiSecret.text.toString(),
            )
            tvStatus.text = "Saved. Poll interval is ${cfg.pollSeconds}s."
        }

        findViewById<Button>(R.id.btnStart).setOnClickListener {
            val intent = Intent(this, LiveRunnerService::class.java).apply {
                action = LiveRunnerService.ACTION_START
            }
            ContextCompat.startForegroundService(this, intent)
            tvStatus.text = "Foreground service started."
        }

        findViewById<Button>(R.id.btnStop).setOnClickListener {
            val intent = Intent(this, LiveRunnerService::class.java).apply {
                action = LiveRunnerService.ACTION_STOP
            }
            startService(intent)
            tvStatus.text = "Foreground service stopped."
        }

        loadConfig()
    }
}
