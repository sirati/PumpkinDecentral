# Starting the development cluster

Build the development executable first:

```bash
nix build .#pumpkin-dev --out-link result-dev
```

## Completion and deployment rule

When implementation work is complete, always produce a fresh Nix development
build and, when the laptop is online, sync that exact build with
`./tools/cluster/sync-to-laptop.sh ./result-dev`.  Cargo builds and checks are
source-level verification only; they are not the server artifact used by this
cluster.  Syncing does not imply permission to restart servers: restart the
local cluster or node 2 on the newest build only when the user asks for it.

The local primary and node 1 do not depend on the laptop transfer. Start them
while the Nix closure is copied to the laptop:

```bash
./tools/cluster/start-local.sh &
local_start=$!
./tools/cluster/sync-to-laptop.sh ./result-dev
wait "$local_start"
```

Do not pass `result-dev` as an argument: the launcher already defaults to
`result-dev/bin/pumpkin`, while an argument must be the executable itself.

The laptop launcher has a pinned `BIN` path. Point it at this build before
starting node 2, then run the launcher:

```bash
remote_bin="$(readlink -f result-dev/bin/pumpkin)"
ssh tslaptop "sed -i \"s|^BIN=.*|BIN=$remote_bin|\" ~/start-pumpkin-node2.sh"
ssh tslaptop '~/start-pumpkin-node2.sh'
```

`start-local.sh` sends Ctrl-C to its existing local tmux sessions and waits up
to 30 seconds for each to exit before launching replacements. This ensures the
previous primary has released its QUIC port. If it times out, it leaves the
existing session in place and refuses to launch a conflicting replacement.

For node 2, send Ctrl-C to its tmux session before running the laptop launcher
again.
