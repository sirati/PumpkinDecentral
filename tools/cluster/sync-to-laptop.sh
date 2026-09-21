#!/usr/bin/env bash
set -euo pipefail
REMOTE="${REMOTE:-tslaptop}"
BUILD_REMOTE=""
if [ "${1:-}" = "--build-remote" ]; then
  BUILD_REMOTE="${2:-.#pumpkin-dev}"
  shift 2
fi
if [ -n "$BUILD_REMOTE" ]; then
  drv="$(nix eval --raw "$BUILD_REMOTE.drvPath")"
  echo "sync-to-laptop: derivation $drv"
  nix copy --derivation --to "ssh://$REMOTE" "$drv"
  echo "sync-to-laptop: copying built inputs (remote pulls cache hits first)"
  nix derivation show "$drv" | python3 -c "
import json, subprocess, sys
drv = json.load(sys.stdin)
info = drv[list(drv)[0]]
paths = set()
for name, spec in info.get('outputs', {}).items():
    pass
for d, spec in info.get('inputs', {}).get('drvs', {}).items():
    for o in spec.get('outputs', []):
        paths.add((d, o))
for key in ('src', 'cargoVendorDir', 'cargoArtifacts'):
    v = info.get('env', {}).get(key, '')
    if v.startswith('/nix/store/'):
        paths.add((v, None))
for d, o in sorted(paths):
    if o is None:
        print(d)
    else:
        r = subprocess.run(['nix', 'derivation', 'show', d], capture_output=True, text=True)
        try:
            out = json.loads(r.stdout)[d]['outputs'][o]['path']
            print(out)
        except Exception as e:
            print(f'skip {d}#{o}: {e}', file=sys.stderr)
" | while IFS= read -r p; do
    [ -n "$p" ] || continue
    if nix path-info --store "ssh://$REMOTE" "$p" >/dev/null 2>&1; then
      echo "remote already has $p"
    else
      nix copy --substitute-on-destination --to "ssh://$REMOTE" "$p" || echo "sync-to-laptop: not built locally, remote will substitute: $p"
    fi
  done
  echo "sync-to-laptop: building final step on $REMOTE"
  ssh "$REMOTE" "nix build '$drv' --no-link --print-out-paths"
  echo "sync-to-laptop: done"
  exit 0
fi
if [ "$#" -eq 0 ]; then
  set -- ./result-dev
fi
for target in "$@"; do
  if [ -e "$target" ]; then
    target="$(readlink -f "$target")"
  fi
  case "$target" in
    /nix/store/*) ;;
    *) echo "sync-to-laptop: refusing non-store path: $target" >&2; exit 1;;
  esac
  echo "sync-to-laptop: $target -> $REMOTE (remote pulls cache hits first)"
  nix copy --substitute-on-destination --to "ssh://$REMOTE" "$target"
done
echo "sync-to-laptop: done"
