#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DIR="$REPO/.cluster-run"
while [ $# -gt 0 ]; do case "$1" in --dir) DIR="$2"; shift 2;; *) echo "usage: $0 [--dir DIR]" >&2; exit 1;; esac; done
shopt -s nullglob
sid_for() { case "$1" in node-primary) echo 0;; node-1) echo 1;; node-2) echo 2;; *) echo "?";; esac; }
node_for() { case "$1" in 0) echo "node-primary";; *) echo "node-$1";; esac; }
bind_for() { grep -E '^[[:space:]]*bind_addr' "$1" 2>/dev/null | head -n 1 | sed -E 's/.*"(.*)".*/\1/'; }
certs=("$DIR"/node-*/cluster-cert.der)
[ "${#certs[@]}" -gt 0 ] || { echo "collect-pins: refusing: no cluster-cert.der under $DIR; boot nodes first" >&2; exit 1; }
missing=0
declare -A PIN ADDR
for cert in "${certs[@]}"; do
  node="$(basename "$(dirname "$cert")")"
  PIN["$node"]="$(sha256sum "$cert" | awk '{print $1}')"
  ADDR["$node"]="$(bind_for "$DIR/$node/pumpkin.toml")"
done
for n in node-primary node-1 node-2; do [ -n "${PIN[$n]:-}" ] || { echo "collect-pins: missing $DIR/$n/cluster-cert.der (boot $n first, then re-run)" >&2; missing=1; }; done
for n in node-primary node-1 node-2; do
  [ -n "${PIN[$n]:-}" ] || continue
  echo "$n (server_id $(sid_for "$n"), addr ${ADDR[$n]:-unknown}) ${PIN[$n]}"
done
echo "--- peers blocks (paste into each node's [cluster] peers = [...]) ---"
for self in node-primary node-1 node-2; do
  line="peers = ["
  first=1
  for peer in node-primary node-1 node-2; do
    [ "$peer" = "$self" ] && continue
    [ -n "${PIN[$peer]:-}" ] || continue
    [ "$first" -eq 1 ] || line="$line, "
    first=0
    line="$line{ server_id = $(sid_for "$peer"), addr = \"${ADDR[$peer]:-<addr>}\", pubkey_sha256_hex = \"${PIN[$peer]}\" }"
  done
  line="$line ]"
  echo "[$self]"
  echo "$line"
done
echo "--- seed sync (secondaries must share the primary seed) ---"
plevel="world"
plname="$({ grep -E '^[[:space:]]*default_level_name' "$DIR/node-primary/pumpkin.toml" 2>/dev/null || true; } | head -n 1 | sed -E 's/.*"(.*)".*/\1/')"
[ -n "${plname:-}" ] && plevel="$plname"
primary_dat="$DIR/node-primary/$plevel/level.dat"
if [ ! -f "$primary_dat" ]; then
  echo "collect-pins: primary level.dat not present yet ($primary_dat missing); boot the primary first, then re-run to sync secondaries"
else
  sha256sum "$primary_dat"
  seed_line="$({ grep -E '^[[:space:]]*seed[[:space:]]*=' "$DIR/node-primary/pumpkin.toml" 2>/dev/null || true; } | head -n 1)"
  for n in node-1 node-2; do
    slevel="world"
    slname="$({ grep -E '^[[:space:]]*default_level_name' "$DIR/$n/pumpkin.toml" 2>/dev/null || true; } | head -n 1 | sed -E 's/.*"(.*)".*/\1/')"
    [ -n "${slname:-}" ] && slevel="$slname"
    secondary_dat="$DIR/$n/$slevel/level.dat"
    mkdir -p "$DIR/$n/$slevel"
    if [ -f "$secondary_dat" ] && cmp -s "$primary_dat" "$secondary_dat"; then
      echo "$n already shares the primary seed ($secondary_dat identical)"
    else
      cp -f "$primary_dat" "$secondary_dat"
      echo "synced primary level.dat into $secondary_dat"
    fi
    sha256sum "$secondary_dat"
    if [ -n "$seed_line" ] && grep -Eq '^[[:space:]]*seed[[:space:]]*=' "$DIR/$n/pumpkin.toml" 2>/dev/null; then
      sed -i -E "s|^[[:space:]]*seed[[:space:]]*=.*|${seed_line//&/\\&}|" "$DIR/$n/pumpkin.toml"
      echo "pinned primary seed in $DIR/$n/pumpkin.toml"
    fi
  done
  echo "node-2 runs remote: copy $DIR/node-2/<world>/level.dat to the laptop before booting it"
fi
[ "$missing" -eq 0 ] || exit 1
