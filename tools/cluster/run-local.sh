#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
NODES=2; BASE_PORT=24577; DIR="$REPO/.cluster-run"
LOCAL_IP="100.88.80.24"; LAPTOP_IP="100.83.143.24"
EU_NTP="europe.pool.ntp.org"; LAPTOP_NTP="asia.pool.ntp.org"
HALF_TICK_MILLIS=25
JAVA_PORT=25565; BEDROCK_PORT=19132
OP_UUID="f611ba39-4a69-492b-9b1e-35c649a7e667"; OP_NAME="sirati97"
MODE="setup"
while [ $# -gt 0 ]; do case "$1" in
  --nodes) NODES="$2"; shift 2;;
  --base-port) BASE_PORT="$2"; shift 2;;
  --dir) DIR="$2"; shift 2;;
  --check) MODE="check"; shift;;
  --start) MODE="start"; shift;;
  *) echo "usage: $0 [--nodes 2] [--base-port P] [--dir DIR] [--check|--start]" >&2; exit 1;;
esac; done
[ "$NODES" -eq 2 ] || { echo "run-local: refusing: two-node run requires --nodes 2" >&2; exit 1; }
mkdir -p "$DIR"
PRIMARY_PORT=$BASE_PORT
P1_PORT=$((BASE_PORT + 1)); P2_PORT=$((BASE_PORT + 2))
P_BIND="$LOCAL_IP:$PRIMARY_PORT"; S1_BIND="$LOCAL_IP:$P1_PORT"; S2_BIND="$LAPTOP_IP:$P2_PORT"
S1_JAVA="$LOCAL_IP:$JAVA_PORT"; S2_JAVA="$LAPTOP_IP:$JAVA_PORT"
S1_BED="$LOCAL_IP:$BEDROCK_PORT"; S2_BED="$LAPTOP_IP:$BEDROCK_PORT"
write_stub() {
  ndir="$1"; role="$2"; sid="$3"; qbind="$4"; jip="$5"; ntp="$6"
  mkdir -p "$ndir" "$ndir/data"
  if [ ! -f "$ndir/pumpkin.toml" ]; then
    if [ "$role" = "primary" ]; then cat > "$ndir/pumpkin.toml" <<TOML
[cluster]
enabled = true
role = "$role"
server_id = $sid
bind_addr = "$qbind"
cert_path = "$ndir/cluster-cert.der"
key_path = "$ndir/cluster-key.der"
peers = [{ server_id = 1, addr = "$S1_BIND", pubkey_sha256_hex = "<paste from collect-pins.sh>" }, { server_id = 2, addr = "$S2_BIND", pubkey_sha256_hex = "<paste from collect-pins.sh>" }]
ntp_servers = ["$ntp"]
max_offset_millis = $HALF_TICK_MILLIS
[networking.java]
enabled = false
[networking.bedrock]
enabled = false
[networking.bedrock.nethernet]
enabled = false
[networking.query]
enabled = false
[networking.rcon]
enabled = false
TOML
    else
      if [ "$sid" -eq 1 ]; then peer_line="peers = [{ server_id = 0, addr = \"$P_BIND\", pubkey_sha256_hex = \"<paste from collect-pins.sh>\" }, { server_id = 2, addr = \"$S2_BIND\", pubkey_sha256_hex = \"<paste from collect-pins.sh>\" }]"; else peer_line="peers = [{ server_id = 0, addr = \"$P_BIND\", pubkey_sha256_hex = \"<paste from collect-pins.sh>\" }, { server_id = 1, addr = \"$S1_BIND\", pubkey_sha256_hex = \"<paste from collect-pins.sh>\" }]"; fi
      cat > "$ndir/pumpkin.toml" <<TOML
[cluster]
enabled = true
role = "$role"
server_id = $sid
bind_addr = "$qbind"
cert_path = "$ndir/cluster-cert.der"
key_path = "$ndir/cluster-key.der"
$peer_line
ntp_servers = ["$ntp"]
max_offset_millis = $HALF_TICK_MILLIS
[networking.java]
enabled = true
address = "$jip:$JAVA_PORT"
[networking.bedrock]
enabled = true
[networking.bedrock.nethernet]
enabled = true
address = "$jip:$BEDROCK_PORT"
[networking.query]
enabled = false
[networking.rcon]
enabled = false
TOML
    fi
  fi
  if [ "$role" = primary ] && [ ! -f "$ndir/data/ops.json" ]; then printf '[{"uuid": "%s", "name": "%s", "level": 4, "bypasses_player_limit": true}]\n' "$OP_UUID" "$OP_NAME" > "$ndir/data/ops.json"; fi
}
write_stub "$DIR/node-primary" primary 0 "$P_BIND" "$LOCAL_IP" "$EU_NTP"
write_stub "$DIR/node-1" secondary 1 "$S1_BIND" "$LOCAL_IP" "$EU_NTP"
write_stub "$DIR/node-2" secondary 2 "$S2_BIND" "$LAPTOP_IP" "$LAPTOP_NTP"
level_dir_name() {
  found="$({ grep -E '^[[:space:]]*default_level_name' "$1" 2>/dev/null || true; } | head -n 1 | sed -E 's/.*"(.*)".*/\1/')"
  [ -n "${found:-}" ] && printf '%s' "$found" || printf 'world'
}
primary_level_dat() { printf '%s' "$DIR/node-primary/$(level_dir_name "$DIR/node-primary/pumpkin.toml")/level.dat"; }
secondary_level_dat() { printf '%s' "$DIR/$1/$(level_dir_name "$DIR/$1/pumpkin.toml")/level.dat"; }
sync_cluster_seed() {
  primary_dat="$(primary_level_dat)"
  [ -f "$primary_dat" ] || { echo "seed sync: no $primary_dat yet; boot the primary first, then re-run to sync secondaries"; return 1; }
  seed_line="$({ grep -E '^[[:space:]]*seed[[:space:]]*=' "$DIR/node-primary/pumpkin.toml" 2>/dev/null || true; } | head -n 1)"
  for n in node-1 node-2; do
    secondary_dat="$(secondary_level_dat "$n")"
    mkdir -p "$(dirname "$secondary_dat")"
    if [ -f "$secondary_dat" ] && cmp -s "$primary_dat" "$secondary_dat"; then
      echo "seed sync: $n already shares the primary seed"
    else
      cp -f "$primary_dat" "$secondary_dat"
      echo "seed sync: copied primary level.dat into $secondary_dat"
    fi
    if [ -n "$seed_line" ] && grep -Eq '^[[:space:]]*seed[[:space:]]*=' "$DIR/$n/pumpkin.toml" 2>/dev/null; then
      sed -i -E "s|^[[:space:]]*seed[[:space:]]*=.*|${seed_line//&/\\&}|" "$DIR/$n/pumpkin.toml"
      echo "seed sync: pinned primary seed in $DIR/$n/pumpkin.toml"
    fi
  done
  echo "seed sync: node-2 runs remote; copy its synced level.dat to the laptop before booting it"
}
wait_for_primary_seed() {
  [ -f "$(primary_level_dat)" ] && return 0
  echo "waiting for primary to generate $(primary_level_dat) ..."
  for _ in $(seq 1 120); do
    sleep 1
    [ -f "$(primary_level_dat)" ] && return 0
  done
  echo "run-local: refusing: primary generated no level.dat within 120s; see $DIR/node-primary/node.log" >&2
  return 1
}
verify_cluster_seed() {
  primary_dat="$(primary_level_dat)"
  [ -f "$primary_dat" ] || { echo "seed check: primary level.dat not present yet; skipping seed comparison"; return 0; }
  for n in node-1 node-2; do
    secondary_dat="$(secondary_level_dat "$n")"
    if [ ! -f "$secondary_dat" ]; then
      echo "run-local: refusing: $secondary_dat missing; re-run $0 (setup) or collect-pins.sh to sync the primary seed" >&2
      errors=$((errors+1))
    elif ! cmp -s "$primary_dat" "$secondary_dat"; then
      echo "run-local: refusing: $secondary_dat differs from primary; re-sync the primary seed before starting" >&2
      errors=$((errors+1))
    fi
  done
}
sec_val() { awk -v sec="[$1]" '$0==sec{f=1;next} /^\[/{f=0} f&&$1=="enabled"{print $3;exit}' "$2"; }
sec_addr() { awk -v sec="[$1]" '$0==sec{f=1;next} /^\[/{f=0} f&&$1=="address"{sub(/^[^"]*"/,"");sub(/".*$/,"");print;exit}' "$2"; }
bind_of() { grep -E '^[[:space:]]*bind_addr' "$1" | head -n 1 | sed -E 's/.*"(.*)".*/\1/'; }
cert_pin() { sha256sum "$1" | awk '{print $1}'; }
peer_pin() { awk -v sid="$2" 'BEGIN{RS="}"} $0 ~ "server_id *= *"sid"([^0-9]|$)" { if (match($0,/pubkey_sha256_hex *= *"[^"]+"/)) { s=substr($0,RSTART,RLENGTH); sub(/^[^"]*"/,"",s); sub(/".*$/,"",s); print s; exit } }' "$1"; }
node_of() { case "$1" in 0) echo "node-primary";; *) echo "node-$1";; esac; }
errors=0
expect_bind() { [ "$(bind_of "$1")" = "$2" ] || { echo "run-local: refusing: $3 bind $(bind_of "$1") != $2" >&2; errors=$((errors+1)); }; }
expect_val() { [ "$(sec_val "$1" "$2")" = "$3" ] || { echo "run-local: refusing: $4 [$1].enabled=$(sec_val "$1" "$2") != $3" >&2; errors=$((errors+1)); }; }
expect_addr() { [ "$(sec_addr "$1" "$2")" = "$3" ] || { echo "run-local: refusing: $4 [$1].address=$(sec_addr "$1" "$2") != $3" >&2; errors=$((errors+1)); }; }
cluster_val() { awk -v key="$1" '$0=="[cluster]"{f=1;next} /^\[/{f=0} f&&$1==key{sub(/^[^=]*= */,"");print;exit}' "$2"; }
expect_cluster() { [ "$(cluster_val "$1" "$2")" = "$3" ] || { echo "run-local: refusing: $4 [cluster].$1=$(cluster_val "$1" "$2") != $3" >&2; errors=$((errors+1)); }; }
PCFG="$DIR/node-primary/pumpkin.toml"; S1CFG="$DIR/node-1/pumpkin.toml"; S2CFG="$DIR/node-2/pumpkin.toml"
expect_bind "$PCFG" "$P_BIND" "primary"
expect_bind "$S1CFG" "$S1_BIND" "node-1"
expect_bind "$S2CFG" "$S2_BIND" "node-2"
expect_val "networking.java" "$PCFG" "false" "primary"
expect_val "networking.bedrock" "$PCFG" "false" "primary"
expect_val "networking.bedrock.nethernet" "$PCFG" "false" "primary"
expect_val "networking.query" "$PCFG" "false" "primary"
expect_val "networking.rcon" "$PCFG" "false" "primary"
expect_val "networking.java" "$S1CFG" "true" "node-1"
expect_addr "networking.java" "$S1CFG" "$S1_JAVA" "node-1"
expect_val "networking.bedrock" "$S1CFG" "true" "node-1"
expect_val "networking.bedrock.nethernet" "$S1CFG" "true" "node-1"
expect_addr "networking.bedrock.nethernet" "$S1CFG" "$S1_BED" "node-1"
expect_val "networking.java" "$S2CFG" "true" "node-2"
expect_addr "networking.java" "$S2CFG" "$S2_JAVA" "node-2"
expect_val "networking.bedrock" "$S2CFG" "true" "node-2"
expect_val "networking.bedrock.nethernet" "$S2CFG" "true" "node-2"
expect_addr "networking.bedrock.nethernet" "$S2CFG" "$S2_BED" "node-2"
expect_cluster "ntp_servers" "$PCFG" "[\"$EU_NTP\"]" "primary"
expect_cluster "max_offset_millis" "$PCFG" "$HALF_TICK_MILLIS" "primary"
expect_cluster "ntp_servers" "$S1CFG" "[\"$EU_NTP\"]" "node-1"
expect_cluster "max_offset_millis" "$S1CFG" "$HALF_TICK_MILLIS" "node-1"
expect_cluster "ntp_servers" "$S2CFG" "[\"$LAPTOP_NTP\"]" "node-2"
expect_cluster "max_offset_millis" "$S2CFG" "$HALF_TICK_MILLIS" "node-2"
check_mesh() {
  for self in 0 1 2; do
    cfg="$DIR/$(node_of "$self")/pumpkin.toml"
    for peer in 0 1 2; do
      [ "$peer" -eq "$self" ] && continue
      want="$(peer_pin "$cfg" "$peer")"
      pcert="$DIR/$(node_of "$peer")/cluster-cert.der"
      case "$want" in ""|*"paste"*|*"PASTE"*|*"xxx"*|*"XXX"*) echo "run-local: refusing: $(node_of "$self") peer $peer pin not wired; run collect-pins.sh" >&2; errors=$((errors+1)); continue;; esac
      case "$want" in *[!0-9a-fA-F]*) echo "run-local: refusing: $(node_of "$self") peer $peer pin malformed: $want" >&2; errors=$((errors+1)); continue;; esac
      [ "${#want}" -eq 64 ] || { echo "run-local: refusing: $(node_of "$self") peer $peer pin length ${#want} != 64" >&2; errors=$((errors+1)); continue; }
      if [ -f "$pcert" ]; then
        have="$(cert_pin "$pcert")"
        [ "$want" = "$have" ] || { echo "run-local: refusing: $(node_of "$self") peer $peer pin mismatch want=$want have=$have" >&2; errors=$((errors+1)); }
      fi
    done
  done
}
have_all=true
for n in node-primary node-1 node-2; do [ -f "$DIR/$n/cluster-cert.der" ] || have_all=false; done
if [ "$MODE" = "setup" ]; then
  [ "$errors" -ne 0 ] && exit 1
  echo "primary  (server_id 0) -> $P_BIND (QUIC only, no Java/Bedrock)"
  echo "secondary (server_id 1) -> $S1_BIND (java $S1_JAVA, bedrock $S1_BED)"
  echo "secondary (server_id 2) -> $S2_BIND (java $S2_JAVA, bedrock $S2_BED)"
  if [ "$have_all" = "true" ]; then check_mesh; [ "$errors" -ne 0 ] && exit 1; echo "pins verified; start: $0 --start --dir $DIR"; else echo "certs incomplete; boot each node once, then run collect-pins.sh --dir $DIR and wire pins"; fi
  sync_cluster_seed || true
  exit 0
fi
check_mesh
[ "$errors" -ne 0 ] && { echo "run-local: refusing to start: fix binds, MC ports, or pins above" >&2; exit 1; }
if [ "$MODE" = "check" ]; then verify_cluster_seed; [ "$errors" -ne 0 ] && exit 1; echo "ok: binds, MC ports, pins, and seed verified"; exit 0; fi
(cd "$DIR/node-primary" && nohup cargo run -q -p pumpkin >"$DIR/node-primary/node.log" 2>&1 & echo $! > "$DIR/node-primary/node.pid")
echo "started node-primary pid $(cat "$DIR/node-primary/node.pid") log $DIR/node-primary/node.log"
wait_for_primary_seed || exit 1
sync_cluster_seed || exit 1
verify_cluster_seed
[ "$errors" -ne 0 ] && { echo "run-local: refusing to start secondaries: seed sync failed above" >&2; exit 1; }
(cd "$DIR/node-1" && nohup cargo run -q -p pumpkin >"$DIR/node-1/node.log" 2>&1 & echo $! > "$DIR/node-1/node.pid")
echo "started node-1 pid $(cat "$DIR/node-1/node.pid") log $DIR/node-1/node.log"
echo "node-2 is remote: copy $DIR/node-2/<world>/level.dat to the laptop, then run: cd <repo> && cargo run -q -p pumpkin (cwd with node-2 pumpkin.toml)"
