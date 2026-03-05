#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

PROJECT_DIR="${1:-$HOME/initializedmodel_2}"
SESSION_NAME="${LIVE_API_TMUX_SESSION:-live-api}"

tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true
tmux new-session -d -s "$SESSION_NAME" "bash '$PROJECT_DIR/deploy/android/termux/run_live_api.sh' '$PROJECT_DIR'"
echo "Started tmux session: $SESSION_NAME"
