#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DIR="$REPO/.cluster-run"
REMOTE="tslaptop"
RDIR="pumpkin-node2"
LOCAL_IP="100.88.80.24"
LAPTOP_IP="100.83.143.24"
cd "$REPO"

echo "=== 1. build ==="
nix build
BIN="$(readlink -f ./result)/bin/pumpkin"
echo "binary: $BIN"

echo "=== 2. copy closure to $REMOTE (cache pulls first) ==="
nix copy --substitute-on-destination --to "ssh://$REMOTE" ./result
echo "copied: $(readlink ./result)"

echo "=== 3. local configs ==="
./tools/cluster/run-local.sh --dir "$DIR"

echo "=== 4. start primary ==="
mkdir -p "$DIR/node-primary"
tmux kill-session -t pumpkin-primary 2>/dev/null || true
tmux new-session -d -s pumpkin-primary -c "$DIR/node-primary" "$BIN"
echo "primary in tmux pumpkin-primary"

echo "=== 5. wait for primary level.dat, sync seed ==="
for _ in $(seq 1 120); do
  dat="$(grep -E '^[[:space:]]*default_level_name' "$DIR/node-primary/pumpkin.toml" 2>/dev/null | head -n 1 | sed -E 's/.*"(.*)".*/\1/')"
  dat="${dat:-world}"
  [ -f "$DIR/node-primary/$dat/level.dat" ] && break
  sleep 1
done
./tools/cluster/run-local.sh --dir "$DIR" >/dev/null || true

echo "=== 6. first-boot local nodes to generate certs ==="
tmux kill-session -t pumpkin-node1 2>/dev/null || true
tmux new-session -d -s pumpkin-node1 -c "$DIR/node-1" "$BIN" || true
sleep 5

echo "=== 7. remote node-2 dir + first boot for cert ==="
ssh "$REMOTE" "mkdir -p ~/$RDIR"
scp "$DIR/node-2/pumpkin.toml" "$REMOTE:~/$RDIR/pumpkin.toml"
ssh "$REMOTE" "tmux kill-session -t pumpkin-node2 2>/dev/null; tmux new-session -d -s pumpkin-node2 -c ~/$RDIR $BIN; sleep 5; tmux capture-pane -t pumpkin-node2 -p | head -n 5"
scp "$REMOTE:~/$RDIR/cluster-cert.der" "$DIR/node-2/cluster-cert.der"

echo "=== 8. wire pins ==="
wire_node() {
  cfg="$DIR/$1/pumpkin.toml"
  for peer in 0 1 2; do
    [ "$peer" = "$2" ] && continue
    pcert="$DIR/$(case "$peer" in 0) echo node-primary;; *) echo "node-$peer";; esac)/cluster-cert.der"
    [ -f "$pcert" ] || { echo "missing $pcert" >&2; return 1; }
    pin="$(sha256sum "$pcert" | awk '{print $1}')"
    python3 - "$cfg" "$peer" "$pin" <<'EOF'
import re, sys
cfg, peer, pin = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(cfg).read()
s2, n = re.subn(r'(\{\s*server_id\s*=\s*' + peer + r'[^}]*?pubkey_sha256_hex\s*=\s*")[^"]*(")', r'\g<1>' + pin + r'\g<2>', s)
assert n == 1, f"pin target not found for peer {peer} in {cfg}"
open(cfg, 'w').write(s2)
EOF
  done
}
wire_node node-primary 0
wire_node node-1 1
wire_node node-2 2
./tools/cluster/collect-pins.sh --dir "$DIR"

echo "=== 9. push node-2 config + seed, restart everything ==="
scp "$DIR/node-2/pumpkin.toml" "$REMOTE:~/$RDIR/pumpkin.toml"
dat="$(grep -E '^[[:space:]]*default_level_name' "$DIR/node-primary/pumpkin.toml" 2>/dev/null | head -n 1 | sed -E 's/.*"(.*)".*/\1/')"
dat="${dat:-world}"
scp "$DIR/node-2/$dat/level.dat" "$REMOTE:~/$RDIR/$dat/level.dat" 2>/dev/null || scp "$DIR/node-primary/$dat/level.dat" "$REMOTE:~/$RDIR/$dat/level.dat"
tmux kill-session -t pumpkin-primary 2>/dev/null || true
tmux new-session -d -s pumpkin-primary -c "$DIR/node-primary" "$BIN"
sleep 3
tmux kill-session -t pumpkin-node1 2>/dev/null || true
tmux new-session -d -s pumpkin-node1 -c "$DIR/node-1" "$BIN"
ssh "$REMOTE" "tmux kill-session -t pumpkin-node2 2>/dev/null; tmux new-session -d -s pumpkin-node2 -c ~/$RDIR $BIN"

echo "=== join ==="
echo "node-1 (this device): $LOCAL_IP:25565"
echo "node-2 (laptop):      $LAPTOP_IP:25565"
