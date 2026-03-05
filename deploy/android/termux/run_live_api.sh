#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

PROJECT_DIR="${1:-$HOME/initializedmodel_2}"

cd "$PROJECT_DIR"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

if [ -f ".env.live" ]; then
  set -a
  # shellcheck disable=SC1091
  source .env.live
  set +a
fi

: "${LIVE_API_AUTH_TOKEN:?LIVE_API_AUTH_TOKEN is required in .env.live}"

LIVE_API_HOST="${LIVE_API_HOST:-127.0.0.1}"
LIVE_API_PORT="${LIVE_API_PORT:-8010}"

exec python -m uvicorn app.live_api:app --host "$LIVE_API_HOST" --port "$LIVE_API_PORT"
