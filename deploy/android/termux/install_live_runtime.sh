#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

PROJECT_DIR="${1:-$HOME/initializedmodel_2}"
SESSION_NAME="${LIVE_API_TMUX_SESSION:-live-api}"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "Project directory not found: $PROJECT_DIR" >&2
  exit 1
fi

pkg update -y
pkg install -y python git tmux termux-api

cd "$PROJECT_DIR"

if [ ! -d ".venv" ]; then
  python -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-live.txt

if [ ! -f ".env.live" ] && [ -f "deploy/android/termux/.env.live.example" ]; then
  cp "deploy/android/termux/.env.live.example" ".env.live"
  echo "Created .env.live from template. Edit LIVE_API_AUTH_TOKEN before start."
fi

termux-wake-lock || true
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true
tmux new-session -d -s "$SESSION_NAME" "bash '$PROJECT_DIR/deploy/android/termux/run_live_api.sh' '$PROJECT_DIR'"

echo "Live API started in tmux session: $SESSION_NAME"
echo "Attach logs: tmux attach -t $SESSION_NAME"
