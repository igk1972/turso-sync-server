#!/usr/bin/env bash
# Builds the images and recreates the soak pod on the podman host from
# .env.podman. The database, the Prometheus data and the backup are kept
# across deploys. The backup goes to Garage inside the pod.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
bucket=turso-soak
backup_url=s3://$bucket/soak
server_image=localhost/turso-sync-server:soak
pod=turso-soak

target() { (set -a && . "$root/.env.podman" && set +a && exec podman --remote "$@"); }

target build --tag "$server_image" "$root"
target build --tag localhost/turso-soak-go:latest --build-arg SERVER_IMAGE="$server_image" "$root/tests/soak/go"
target build --tag localhost/turso-soak:latest --build-arg SERVER_IMAGE="$server_image" "$root/tests/soak"
target build --tag localhost/turso-soak-prometheus:latest \
    --file "$root/tests/soak/prometheus.Dockerfile" "$root/tests/soak"
target build --tag localhost/turso-soak-garage:latest --file "$root/tests/soak/garage.Dockerfile" "$root/tests/soak"
target pull docker.io/library/caddy:2.11.7

# The server uploads the rest of the WAL on shutdown.
target pod stop --ignore --time 30 "$pod" >/dev/null
target pod rm --force --ignore "$pod" >/dev/null
# Garage keeps its data across deploys, so its keys must stay the same.
secret_once() {
    target secret inspect "$1" >/dev/null 2>&1 || printf %s "$2" | target secret create "$1" - >/dev/null
}
secret_once turso-soak-garage-key "GK$(openssl rand -hex 16)"
secret_once turso-soak-garage-secret "$(openssl rand -hex 32)"
secret_once turso-soak-garage-rpc "$(openssl rand -hex 32)"
# The server and its clients are recreated together, so a new token is fine.
openssl rand -hex 32 | tr -d '\n' | target secret create --replace turso-soak-token - >/dev/null
for volume in turso-soak-data turso-soak-prometheus turso-soak-garage; do
    target volume create --ignore "$volume" >/dev/null
done

# Limits keep the soak from starving the other services on the host.
target pod create --name "$pod" --cpus 2 --memory 4g \
    --publish 127.0.0.1:19090:9090 --publish 127.0.0.1:18000:8000 >/dev/null

keys=(
    --secret turso-soak-garage-key,type=env,target=AWS_ACCESS_KEY_ID
    --secret turso-soak-garage-secret,type=env,target=AWS_SECRET_ACCESS_KEY
    --env AWS_REGION=garage --env AWS_ENDPOINT=http://127.0.0.1:3900 --env AWS_ALLOW_HTTP=true
)
# Only the server and the clients get the keys and the token.
access=("${keys[@]}" --secret turso-soak-token,type=env,target=TURSO_SYNC_AUTH_TOKEN)
run() { target run --detach --pod "$pod" --restart always --log-opt max-size=20m "$@" >/dev/null; }

run --name "$pod-s3" --memory 256m --volume turso-soak-garage:/var/lib/garage \
    --secret turso-soak-garage-key,type=env,target=GARAGE_DEFAULT_ACCESS_KEY \
    --secret turso-soak-garage-secret,type=env,target=GARAGE_DEFAULT_SECRET_KEY \
    --secret turso-soak-garage-rpc,type=env,target=GARAGE_RPC_SECRET \
    --env GARAGE_DEFAULT_BUCKET="$bucket" --env RUST_LOG=warn \
    localhost/turso-soak-garage:latest /garage server --single-node --default-bucket

run --name "$pod-server" --memory 512m --stop-timeout 30 --volume turso-soak-data:/data "${access[@]}" \
    --env RUST_LOG=warn,turso_sync_server=info,turso_sync_server::http=warn \
    --env TURSO_SYNC_LISTEN=127.0.0.1:8080 \
    --env TURSO_SYNC_METRICS_LISTEN=127.0.0.1:9464 \
    --env TURSO_SYNC_BACKUP_URL="$backup_url" \
    --env TURSO_SYNC_BACKUP_RETENTION_DAYS=3 \
    "$server_image" /data/soak.db
run --name "$pod-proxy" --memory 64m docker.io/library/caddy:2.11.7 \
    caddy reverse-proxy --from :8000 --to 127.0.0.1:8080
run --name "$pod-prometheus" --memory 256m --volume turso-soak-prometheus:/prometheus \
    localhost/turso-soak-prometheus:latest \
    --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=30d
# Every client holds a copy of the database in memory per connection.
run --name "$pod-client" --memory 1g "${access[@]}" --env SOAK_BACKUP_URL="$backup_url" \
    localhost/turso-soak:latest
run --name "$pod-client-go" --memory 1g "${access[@]}" --env SOAK_BACKUP_URL="$backup_url" \
    localhost/turso-soak-go:latest

target ps --pod --filter pod="$pod" --format '{{.Names}}\t{{.Status}}'
