# Android Live Runner Deployment

## 1. Scope

This deployment isolates live robot execution into `app/live_api.py` and keeps backtest/optimizer out of the Android runtime.

Included endpoints:

- `POST /api/live/robots`
- `GET /api/live/robots`
- `GET /api/live/robots/{robot_id}`
- `POST /api/live/robots/{robot_id}/start`
- `POST /api/live/robots/{robot_id}/stop`
- `POST /api/live/robots/{robot_id}/close-all`
- `POST /api/live/robots/{robot_id}/status-check`
- `DELETE /api/live/robots/{robot_id}`
- `GET /api/live/robots/{robot_id}/events`
- `GET /api/live/health`
- `GET /healthz`

## 2. Security Baseline

- Token auth enabled by default (`LIVE_API_REQUIRE_AUTH=true`).
- Configure a strong token (`LIVE_API_AUTH_TOKEN`).
- `LIVE_API_ALLOWED_ORIGINS` does not allow `*`.
- Android app stores exchange credentials with `EncryptedSharedPreferences` (Android Keystore backed).

## 3. Termux MVP (recommended first)

1. Copy project to Android Termux home.
2. Run:

```bash
chmod +x deploy/android/termux/*.sh
bash deploy/android/termux/install_live_runtime.sh ~/initializedmodel_2
```

3. Edit `~/initializedmodel_2/.env.live` and set:

- `LIVE_API_AUTH_TOKEN`
- `BYBIT_API_KEY`
- `BYBIT_API_SECRET`
- `MOBILE_NOTIFY_NTFY_TOPIC`

4. Restart session:

```bash
bash deploy/android/termux/start_live_api_tmux.sh ~/initializedmodel_2
```

5. Check health:

```bash
curl http://127.0.0.1:8010/healthz
curl -H "Authorization: Bearer <LIVE_API_AUTH_TOKEN>" http://127.0.0.1:8010/api/live/health
```

## 4. Android App Runtime Model

- Foreground service runs continuously and polls every 5-10 seconds.
- On boot (`BOOT_COMPLETED`), service restarts automatically.
- If API health fails repeatedly, service triggers Termux script to restart the Python runner.
- Status updates use `POST /api/live/robots/{robot_id}/status-check`.
- Push notifications use backend `ntfy` provider.

## 5. Release Gates

1. `dry-run` stress test for 48 hours.
2. Small capital live test.
3. Scale position size gradually.

## 6. Ops Checklist

- Keep device on power and disable battery optimization for the app and Termux.
- Enable `termux-wake-lock`.
- Verify `status-check` and event stream after device reboot.
- Keep API token rotated and never hardcode keys in source.
