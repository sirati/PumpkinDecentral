# Local cluster tooling

Local multi-node setup for the decentral Pumpkin build.

- Each node generates its QUIC cert/key locally on first boot
  (see `pumpkin-cluster` cert module).
- Public certs are exchanged out of band ("third channel", e.g. pasted
  by the operator doing the setup). This tooling only collects the
  pins and wires configs together; it never invents keys.
- `run-local.sh` boots one disk-only primary plus N secondaries on
  loopback with distinct ports and data dirs.
- `collect-pins.sh` prints `server_id addr sha256` lines used to fill
  the `peers:` section of each `pumpkin.toml`.

Example:

```sh
./tools/cluster/run-local.sh --nodes 2 --base-port 24577 --dir /tmp/pumpkin-cluster
./tools/cluster/collect-pins.sh --dir /tmp/pumpkin-cluster
```
