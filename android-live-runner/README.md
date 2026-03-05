# Android Live Runner (MVP)

Minimal Android app that:

- starts a foreground service for live monitoring,
- auto-starts on boot,
- polls `/api/live/robots/{robot_id}/status-check` every 5-10 seconds,
- stores exchange credentials in `EncryptedSharedPreferences` (Keystore backed),
- can trigger Termux script to restart Python live API runner when health checks fail.

## Build

1. Open `android-live-runner` in Android Studio.
2. Sync Gradle and build `app`.
3. Install on a device with Android 8.0+.

## Runtime Requirements

- Termux installed (for MVP process bootstrap).
- Live API running from project scripts:
  - `deploy/android/termux/install_live_runtime.sh`
  - `deploy/android/termux/start_live_api_tmux.sh`
- API token configured (`LIVE_API_AUTH_TOKEN`) and mirrored in app settings.

## Service Model

- `LiveRunnerService` runs as `START_STICKY`.
- `BootReceiver` restarts service on `BOOT_COMPLETED` and app upgrade.
- polling interval is clamped to `5..10` seconds.

## Security Notes

- Do not leave `LIVE_API_AUTH_TOKEN` empty.
- Do not set `LIVE_API_ALLOWED_ORIGINS=*`.
- Keep API key/secret only in secure store; avoid plaintext files on Android app side.
