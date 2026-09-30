#!/bin/sh
# Check the binary's recorded build settings, including cross-compiled targets.
set -eu

if [ "$#" -eq 0 ]; then
  echo 'usage: check-cgo-disabled.sh <binary> ...' >&2
  exit 2
fi

for binary do
  settings=$(go version -m "$binary")
  if ! printf '%s\n' "$settings" | awk '
    $1 == "build" && $2 == "CGO_ENABLED=0" { found = 1 }
    END { exit !found }
  '; then
    echo "Refusing to distribute $binary: expected CGO_ENABLED=0 in build metadata" >&2
    exit 1
  fi
  echo "Verified CGO_ENABLED=0: $binary"
done
