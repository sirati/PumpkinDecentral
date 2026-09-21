# Local cluster tooling

Local multi-node setup for the decentral Pumpkin build.

- Each node generates its QUIC cert/key locally on first boot
  (see `pumpkin-cluster` cert module).
- Public certs are exchanged out of band ("third channel", e.g. pasted
  by the operator doing the setup). This tooling only collects the
  pins and wires configs together; it never invents keys.
- `run-local.sh` is the existing split-machine workflow: node 2 uses the
  laptop address and is launched remotely.
- `run-all-local.sh` is the isolated integration workflow: one disk-only
  primary and two secondaries bind only to `127.0.0.1`; the secondaries expose
  Java at distinct ports in Minecraft offline mode for a local protocol bot.
- `collect-pins.sh` prints `server_id addr sha256` lines used to fill
  the `peers:` section of each `pumpkin.toml`.

Example:

```sh
./tools/cluster/run-local.sh --nodes 2 --base-port 24577 --dir /tmp/pumpkin-cluster
./tools/cluster/collect-pins.sh --dir /tmp/pumpkin-cluster
```

For an all-local Java integration run, first build the development executable,
then create the isolated configuration and start it:

```sh
nix build .#pumpkin-dev --out-link result-dev
./tools/cluster/run-all-local.sh
./tools/cluster/run-all-local.sh --start
```

The first `--start` uses Pumpkin's normal first-boot certificate generation
for each node, wires the resulting local pins, and starts three dedicated
tmux sessions. `endpoints.env` in the selected cluster directory gives a bot
harness the two Java endpoints. The primary has no Java, Bedrock, query, or
RCON listener; the default secondary Java endpoints are `127.0.0.1:25565` and
`127.0.0.1:25566` with `online_mode = false` and `encryption = false`.
“Offline mode” here means Minecraft offline authentication, not a change to
the cluster clock requirement: this launcher retains the configured NTP pool.
A fully network-isolated bot fixture may instead configure its nodes to use a
loopback SNTP responder.
