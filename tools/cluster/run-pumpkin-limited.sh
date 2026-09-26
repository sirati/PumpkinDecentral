#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -x "$1" ]; then
  echo "usage: $0 /path/to/pumpkin" >&2
  exit 2
fi

exec systemd-run --user --scope --quiet --collect -p MemoryMax=1G -- "$1"
