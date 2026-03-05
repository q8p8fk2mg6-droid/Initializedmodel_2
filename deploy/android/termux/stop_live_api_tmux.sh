#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

SESSION_NAME="${LIVE_API_TMUX_SESSION:-live-api}"
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true
echo "Stopped tmux session: $SESSION_NAME"
