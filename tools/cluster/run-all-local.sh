#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DIR="$REPO/.cluster-all-local"
QUIC_BASE_PORT=24677
JAVA_BASE_PORT=25565
BIN="${PUMPKIN_BIN:-$REPO/result-dev/bin/pumpkin}"
MODE="setup"

usage() {
  echo "usage: $0 [--dir DIR] [--quic-base-port PORT] [--java-base-port PORT] [--bin PATH] [--check|--start]" >&2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir) DIR="$2"; shift 2 ;;
    --quic-base-port) QUIC_BASE_PORT="$2"; shift 2 ;;
    --java-base-port) JAVA_BASE_PORT="$2"; shift 2 ;;
    --bin) BIN="$2"; shift 2 ;;
    --check) MODE="check"; shift ;;
    --start) MODE="start"; shift ;;
    *) usage; exit 1 ;;
  esac
done

port_range() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) [ "$1" -ge 1 ] && [ "$1" -le 65533 ] ;;
  esac
}

port_range "$QUIC_BASE_PORT" || { echo "run-all-local: invalid QUIC base port: $QUIC_BASE_PORT" >&2; exit 1; }
port_range "$JAVA_BASE_PORT" || { echo "run-all-local: invalid Java base port: $JAVA_BASE_PORT" >&2; exit 1; }

PRIMARY_QUIC="127.0.0.1:$QUIC_BASE_PORT"
NODE1_QUIC="127.0.0.1:$((QUIC_BASE_PORT + 1))"
NODE2_QUIC="127.0.0.1:$((QUIC_BASE_PORT + 2))"
NODE1_JAVA="127.0.0.1:$JAVA_BASE_PORT"
NODE2_JAVA="127.0.0.1:$((JAVA_BASE_PORT + 1))"

node_dir() {
  case "$1" in
    0) printf '%s/node-primary' "$DIR" ;;
    1) printf '%s/node-1' "$DIR" ;;
    2) printf '%s/node-2' "$DIR" ;;
  esac
}

node_name() {
  case "$1" in
    0) printf 'node-primary' ;;
    1) printf 'node-1' ;;
    2) printf 'node-2' ;;
  esac
}

node_quic() {
  case "$1" in
    0) printf '%s' "$PRIMARY_QUIC" ;;
    1) printf '%s' "$NODE1_QUIC" ;;
    2) printf '%s' "$NODE2_QUIC" ;;
  esac
}

node_config() {
  printf '%s/pumpkin.toml' "$(node_dir "$1")"
}

write_primary_config() {
  local config="$1"
  cat > "$config" <<TOML
seed = "1787949120387005756"
default_level_name = "world"

[logging]
enabled = true
level = "info"
file = "node.log"

[world]
autosave_ticks = 6000

[world.chunk]
type = "anvil"
write_in_place = false

[networking.java]
enabled = false
address = "$NODE1_JAVA"
online_mode = false
encryption = false

[networking.bedrock]
enabled = false

[networking.bedrock.nethernet]
enabled = false
address = "127.0.0.1:19132"

[networking.query]
enabled = false
address = "127.0.0.1:25565"

[networking.rcon]
enabled = false
address = "127.0.0.1:25575"

[networking.lan_broadcast]
enabled = false

[player_data]
save_player_data = true

[cluster]
enabled = true
role = "primary"
server_id = 0
primary_server_id = 0
bind_addr = "$PRIMARY_QUIC"
cert_path = "cluster-cert.der"
key_path = "cluster-key.der"
peers = []
ntp_servers = ["europe.pool.ntp.org"]
max_precision_millis = 25

[telemetry]
enabled = false
TOML
}

write_secondary_config() {
  local config="$1"
  local server_id="$2"
  local quic_addr="$3"
  local java_addr="$4"
  cat > "$config" <<TOML
seed = "1787949120387005756"
default_level_name = "world"

[logging]
enabled = true
level = "info"
file = "node.log"

[world]
autosave_ticks = 6000

[world.chunk]
type = "anvil"
write_in_place = false

[networking.java]
enabled = true
address = "$java_addr"
online_mode = false
encryption = false

[networking.bedrock]
enabled = false

[networking.bedrock.nethernet]
enabled = false
address = "127.0.0.1:19132"

[networking.query]
enabled = false
address = "127.0.0.1:25565"

[networking.rcon]
enabled = false
address = "127.0.0.1:25575"

[networking.lan_broadcast]
enabled = false

[player_data]
save_player_data = false

[cluster]
enabled = true
role = "secondary"
server_id = $server_id
primary_server_id = 0
bind_addr = "$quic_addr"
cert_path = "cluster-cert.der"
key_path = "cluster-key.der"
peers = []
ntp_servers = ["europe.pool.ntp.org"]
max_precision_millis = 25

[telemetry]
enabled = false
TOML
}

write_config_if_missing() {
  local id="$1"
  local ndir
  local config
  ndir="$(node_dir "$id")"
  config="$ndir/pumpkin.toml"
  mkdir -p "$ndir"
  [ ! -f "$config" ] || return 0
  case "$id" in
    0) write_primary_config "$config" ;;
    1) write_secondary_config "$config" 1 "$NODE1_QUIC" "$NODE1_JAVA" ;;
    2) write_secondary_config "$config" 2 "$NODE2_QUIC" "$NODE2_JAVA" ;;
  esac
}

write_endpoints() {
  cat > "$DIR/endpoints.env" <<ENV
PRIMARY_ROLE=primary
PRIMARY_QUIC_ADDR=$PRIMARY_QUIC
PRIMARY_JAVA_ENABLED=false
PRIMARY_BEDROCK_ENABLED=false
SECONDARY_1_ROLE=secondary
SECONDARY_1_QUIC_ADDR=$NODE1_QUIC
SECONDARY_1_JAVA_ADDR=$NODE1_JAVA
SECONDARY_1_JAVA_ONLINE_MODE=false
SECONDARY_1_JAVA_ENCRYPTION=false
SECONDARY_2_ROLE=secondary
SECONDARY_2_QUIC_ADDR=$NODE2_QUIC
SECONDARY_2_JAVA_ADDR=$NODE2_JAVA
SECONDARY_2_JAVA_ONLINE_MODE=false
SECONDARY_2_JAVA_ENCRYPTION=false
ENV
}

toml_value() {
  local config="$1"
  local section="$2"
  local key="$3"
  awk -v section="[$section]" -v key="$key" '
    $0 == section { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && $1 == key {
      sub(/^[^=]*=[[:space:]]*/, "")
      print
      exit
    }
  ' "$config"
}

expect_value() {
  local config="$1"
  local section="$2"
  local key="$3"
  local want="$4"
  local node="$5"
  local have
  have="$(toml_value "$config" "$section" "$key")"
  [ "$have" = "$want" ] || {
    echo "run-all-local: refusing: $node [$section].$key=$have, expected $want" >&2
    return 1
  }
}

check_static_config() {
  local failures=0
  local config
  config="$(node_config 0)"
  expect_value "$config" cluster role '"primary"' node-primary || failures=1
  expect_value "$config" cluster server_id 0 node-primary || failures=1
  expect_value "$config" cluster bind_addr "\"$PRIMARY_QUIC\"" node-primary || failures=1
  expect_value "$config" networking.java enabled false node-primary || failures=1
  expect_value "$config" networking.bedrock enabled false node-primary || failures=1
  expect_value "$config" networking.bedrock.nethernet enabled false node-primary || failures=1
  expect_value "$config" networking.query enabled false node-primary || failures=1
  expect_value "$config" networking.rcon enabled false node-primary || failures=1
  expect_value "$config" player_data save_player_data true node-primary || failures=1
  for id in 1 2; do
    config="$(node_config "$id")"
    local java_addr
    java_addr="$NODE1_JAVA"
    [ "$id" -eq 2 ] && java_addr="$NODE2_JAVA"
    expect_value "$config" cluster role '"secondary"' "$(node_name "$id")" || failures=1
    expect_value "$config" cluster server_id "$id" "$(node_name "$id")" || failures=1
    expect_value "$config" cluster bind_addr "\"$(node_quic "$id")\"" "$(node_name "$id")" || failures=1
    expect_value "$config" networking.java enabled true "$(node_name "$id")" || failures=1
    expect_value "$config" networking.java address "\"$java_addr\"" "$(node_name "$id")" || failures=1
    expect_value "$config" networking.java online_mode false "$(node_name "$id")" || failures=1
    expect_value "$config" networking.java encryption false "$(node_name "$id")" || failures=1
    expect_value "$config" networking.bedrock enabled false "$(node_name "$id")" || failures=1
    expect_value "$config" networking.bedrock.nethernet enabled false "$(node_name "$id")" || failures=1
    expect_value "$config" networking.query enabled false "$(node_name "$id")" || failures=1
    expect_value "$config" networking.rcon enabled false "$(node_name "$id")" || failures=1
    expect_value "$config" player_data save_player_data false "$(node_name "$id")" || failures=1
  done
  [ "$failures" -eq 0 ]
}

cert_pin() {
  sha256sum "$1" | awk '{print $1}'
}

peer_line() {
  local self="$1"
  local line='peers = ['
  local first=true
  local peer
  for peer in 0 1 2; do
    [ "$peer" -eq "$self" ] && continue
    [ "$first" = true ] || line+=', '
    first=false
    line+="{ server_id = $peer, addr = \"$(node_quic "$peer")\", pubkey_sha256_hex = \"$(cert_pin "$(node_dir "$peer")/cluster-cert.der")\" }"
  done
  printf '%s ]' "$line"
}

replace_peers() {
  local config="$1"
  local peers="$2"
  local temporary
  temporary="$(mktemp "$config.XXXXXX")"
  awk -v peers="$peers" '
    $0 == "[cluster]" { in_cluster = 1 }
    /^\[/ && $0 != "[cluster]" { in_cluster = 0 }
    in_cluster && $1 == "peers" { print peers; replaced = 1; next }
    { print }
    END { if (!replaced) exit 1 }
  ' "$config" > "$temporary" || { rm -f "$temporary"; return 1; }
  mv "$temporary" "$config"
}

have_complete_keypair() {
  local id="$1"
  local ndir
  ndir="$(node_dir "$id")"
  [ -f "$ndir/cluster-cert.der" ] && [ -f "$ndir/cluster-key.der" ]
}

bootstrap_keypair() {
  local id="$1"
  local ndir
  ndir="$(node_dir "$id")"
  if have_complete_keypair "$id"; then
    return 0
  fi
  if [ -e "$ndir/cluster-cert.der" ] || [ -e "$ndir/cluster-key.der" ]; then
    echo "run-all-local: refusing: $(node_name "$id") has an incomplete cluster keypair in $ndir" >&2
    return 1
  fi
  echo "run-all-local: generating the local certificate for $(node_name "$id") through Pumpkin's first-boot path"
  set +e
  (cd "$ndir" && timeout --foreground --preserve-status --signal=INT --kill-after=5s 30s "$BIN") > "$ndir/cert-bootstrap.log" 2>&1
  local status=$?
  set -e
  if ! have_complete_keypair "$id"; then
    echo "run-all-local: $(node_name "$id") did not create a complete keypair (exit $status); see $ndir/cert-bootstrap.log" >&2
    return 1
  fi
}

wire_pins() {
  local id
  for id in 0 1 2; do
    replace_peers "$(node_config "$id")" "$(peer_line "$id")" || {
      echo "run-all-local: failed to wire peers for $(node_name "$id")" >&2
      return 1
    }
  done
}

check_mesh() {
  local id
  local config
  local want
  for id in 0 1 2; do
    have_complete_keypair "$id" || {
      echo "run-all-local: missing complete keypair for $(node_name "$id")" >&2
      return 1
    }
    config="$(node_config "$id")"
    want="$(peer_line "$id")"
    grep -Fqx "$want" "$config" || {
      echo "run-all-local: peer pins for $(node_name "$id") are not wired for this all-local mesh" >&2
      return 1
    }
  done
}

print_layout() {
  echo "all-local cluster directory: $DIR"
  echo "primary server_id=0 role=primary disk-persistence=enabled QUIC/UDP=$PRIMARY_QUIC Java=disabled Bedrock=disabled query=disabled rcon=disabled"
  echo "node-1 server_id=1 role=secondary QUIC/UDP=$NODE1_QUIC Java/TCP=$NODE1_JAVA online_mode=false encryption=false Bedrock=disabled"
  echo "node-2 server_id=2 role=secondary QUIC/UDP=$NODE2_QUIC Java/TCP=$NODE2_JAVA online_mode=false encryption=false Bedrock=disabled"
  echo "bot endpoints: $NODE1_JAVA and $NODE2_JAVA"
  echo "endpoint environment: $DIR/endpoints.env"
}

start_node() {
  local id="$1"
  local session="pumpkin-all-local-$(node_name "$id")"
  local command
  printf -v command 'exec %q' "$BIN"
  tmux kill-session -t "$session" 2>/dev/null || true
  tmux new-session -d -s "$session" -c "$(node_dir "$id")" "$command"
}

for id in 0 1 2; do
  write_config_if_missing "$id"
done
write_endpoints
check_static_config

if [ "$MODE" = "setup" ]; then
  print_layout
  if check_mesh 2>/dev/null; then
    echo "pins are wired; start with: $0 --start --dir $DIR --bin $BIN"
  else
    echo "setup is written; the first --start generates each node's local certificate, wires pins, then launches the mesh"
  fi
  exit 0
fi

if [ "$MODE" = "check" ]; then
  check_mesh
  print_layout
  echo "ok: all-local roles, listeners, loopback binds, certificate pins, and bot endpoints are ready"
  exit 0
fi

BIN="$(readlink -f "$BIN")"
[ -x "$BIN" ] || { echo "run-all-local: executable not found: $BIN" >&2; exit 1; }
for id in 0 1 2; do
  bootstrap_keypair "$id"
done
wire_pins
check_mesh
start_node 0
start_node 1
start_node 2
print_layout
echo "started tmux sessions: pumpkin-all-local-node-primary, pumpkin-all-local-node-1, pumpkin-all-local-node-2"
