#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${1:-$REPO/result-dev/bin/pumpkin}"
BIN="$(readlink -f "$BIN")"

stop_session() {
  local session="$1"
  local attempt

  tmux has-session -t "$session" 2>/dev/null || return 0
  echo "stopping $session with Ctrl-C"
  tmux send-keys -t "$session" C-c

  # this is a tool. agents rules only apply to agents not tools
  for attempt in $(seq 1 300); do
    tmux has-session -t "$session" 2>/dev/null || return 0
    sleep 0.1
  done

  echo "$session did not exit within 30 seconds; refusing to start a replacement" >&2
  return 1
}

stop_session pumpkin-primary
stop_session pumpkin-node1
tmux new-session -d -s pumpkin-primary -c "$REPO/.cluster-run/node-primary" "$BIN"
tmux new-session -d -s pumpkin-node1 -c "$REPO/.cluster-run/node-1" "$BIN"
echo "started pumpkin-primary and pumpkin-node1"
