#!/bin/sh
# Runs the sync tests of the Go driver against this server. They create files
# in the working directory, so they run from a writable copy of the module.
set -eu
cd "$(dirname "$0")"
LOCAL_SYNC_SERVER=${LOCAL_SYNC_SERVER:-$(cd ../.. && pwd)/target/release/turso-sync-server}
# The driver's harness starts the server without a database argument.
TURSO_SYNC_DB=${TURSO_SYNC_DB:-:memory:}
export LOCAL_SYNC_SERVER TURSO_SYNC_DB

src=$(go list -m -f '{{.Dir}}' turso.tech/database/tursogo)
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
cp -R "$src/." "$dir"
chmod -R u+w "$dir"
cd "$dir"
go test -count=1 -run '^TestSync' "$@" .
