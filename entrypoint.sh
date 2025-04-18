#!/bin/sh

set -e

PG_VERSION=${PG_VERSION:-16}

# Validate input
if [ "$PG_VERSION" != "15" ] && [ "$PG_VERSION" != "16" ]; then
  echo "Error: PG_VERSION must be either 15 or 16" >&2
  exit 1
fi

# Update symlinks to point to the requested version
for cmd in psql pg_dump pg_restore pg_dumpall; do
  ln -sf "/usr/lib/postgresql/$PG_VERSION/bin/$cmd" "/usr/local/bin/$cmd"
done

onedump "$@"