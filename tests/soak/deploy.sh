#!/usr/bin/env bash
# Builds the images and recreates the soak pod. The database, the Prometheus
# data and the backup are kept across deploys.
#
# The server image is built on the podman host from .env.podman, since a Rust
# build does not fit on a small host, and copied to the host from .env.scaleway,
# which also holds the S3 keys (S3_ENDPOINT, S3_REGION, S3_ACCESSKEY, S3_SECRETKEY).
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
backup_url=${SOAK_BACKUP_URL:-s3://igk-turso-sync-soak/soak}
server_image=localhost/turso-sync-server:soak
pod=turso-soak

remote() {
    env_file=$1
    shift
    (set -a && . "$env_file" && set +a && exec podman --remote "$@")
}
builder() { remote "$root/.env.podman" "$@"; }
target() { remote "$root/.env.scaleway" "$@"; }

. "$root/.env.scaleway"

builder build --tag "$server_image" "$root"
builder save "$server_image" | target load
target build --tag localhost/turso-soak:latest --build-arg SERVER_IMAGE="$server_image" "$root/tests/soak"
target build --tag localhost/turso-soak-prometheus:latest \
    --file "$root/tests/soak/prometheus.Dockerfile" "$root/tests/soak"
target pull docker.io/library/caddy:2.11.7

# The server uploads the rest of the WAL on shutdown.
target pod stop --ignore --time 30 "$pod" >/dev/null
target pod rm --force --ignore "$pod" >/dev/null
printf %s "$S3_ACCESSKEY" | target secret create --replace turso-soak-s3-key - >/dev/null
printf %s "$S3_SECRETKEY" | target secret create --replace turso-soak-s3-secret - >/dev/null
# The server and its clients are recreated together, so a new token is fine.
openssl rand -hex 32 | tr -d '\n' | target secret create --replace turso-soak-token - >/dev/null
target volume create --ignore turso-soak-data >/dev/null
target volume create --ignore turso-soak-prometheus >/dev/null

# Limits keep the soak from starving the other services on the host.
target pod create --name "$pod" --cpus 1 --memory 640m \
    --publish 127.0.0.1:19090:9090 --publish 127.0.0.1:18000:8000 >/dev/null

# Only the server and the client get the keys and the token.
access=(
    --env AWS_REGION="$S3_REGION" --env AWS_ENDPOINT="$S3_ENDPOINT"
    --secret turso-soak-s3-key,type=env,target=AWS_ACCESS_KEY_ID
    --secret turso-soak-s3-secret,type=env,target=AWS_SECRET_ACCESS_KEY
    --secret turso-soak-token,type=env,target=TURSO_SYNC_AUTH_TOKEN
)
run() { target run --detach --pod "$pod" --restart always --log-opt max-size=20m "$@" >/dev/null; }

run --name "$pod-server" --memory 256m --stop-timeout 30 --volume turso-soak-data:/data "${access[@]}" \
    --env RUST_LOG=warn,turso_sync_server=info,turso_sync_server::http=warn \
    --env TURSO_SYNC_LISTEN=127.0.0.1:8080 \
    --env TURSO_SYNC_METRICS_LISTEN=127.0.0.1:9464 \
    --env TURSO_SYNC_BACKUP_URL="$backup_url" \
    --env TURSO_SYNC_BACKUP_RETENTION_DAYS=3 \
    "$server_image" /data/soak.db
run --name "$pod-proxy" --memory 64m docker.io/library/caddy:2.11.7 \
    caddy reverse-proxy --from :8000 --to 127.0.0.1:8080
run --name "$pod-prometheus" --memory 192m --volume turso-soak-prometheus:/prometheus \
    localhost/turso-soak-prometheus:latest \
    --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=30d
run --name "$pod-client" --memory 384m "${access[@]}" --env SOAK_BACKUP_URL="$backup_url" \
    localhost/turso-soak:latest

target ps --pod --filter pod="$pod" --format '{{.Names}}\t{{.Status}}'
