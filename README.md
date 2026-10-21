# turso-sync-server

A sync server for [Turso](https://github.com/tursodatabase/turso) clients (`@tursodatabase/sync`, the Go driver `turso.tech/database/tursogo`). It speaks the same protocol as `tursodb --sync-server` and adds what a production deployment needs:

- **an access token** (`Authorization: Bearer`) and a request timeout;
- **concurrent request handling**, so one slow client no longer blocks the others;
- **periodic checkpoints that do not break clients**: every checkpoint starts a new generation, and a client from an older generation receives only the pages that changed;
- **continuous backup** to S3 or a directory, with restore to any point in time, modelled on sqld's bottomless;
- **Prometheus metrics**.

Client and server versions are aligned: the server is built on `turso_core` 0.8.3 and tested with `@tursodatabase/sync` 0.8.3 and `tursogo` v0.8.3.

## Quick start

```sh
docker build -t turso-sync-server .
docker run -d --name turso-sync -p 8080:8080 -v turso-data:/data \
  -e TURSO_SYNC_AUTH_TOKEN=secret \
  turso-sync-server
```

The database lives in `/data/main.db`. The image runs as uid 10001: a named volume gets the right ownership automatically, while a host directory has to be given to that uid (`chown 10001`).

A compose example with backups to Scaleway and metrics:

```yaml
services:
  turso-sync:
    build: .
    ports: ["8080:8080"]
    volumes: ["turso-data:/data"]
    environment:
      TURSO_SYNC_AUTH_TOKEN_FILE: /run/secrets/turso_token
      TURSO_SYNC_BACKUP_URL: s3://my-bucket/turso/main
      TURSO_SYNC_BACKUP_RETENTION_DAYS: "30"
      TURSO_SYNC_METRICS_LISTEN: 0.0.0.0:9090   # not published; Prometheus scrapes it
      AWS_REGION: fr-par
      AWS_ENDPOINT: https://s3.fr-par.scw.cloud
      AWS_ACCESS_KEY_ID: ${S3_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET}
    secrets: [turso_token]
    # On shutdown the server uploads the latest changes to the backup.
    stop_grace_period: 30s

secrets:
  turso_token:
    file: ./turso_token.txt   # must be readable by uid 10001

volumes:
  turso-data:
```

The server does not terminate TLS; put a reverse proxy (Caddy, nginx, Traefik) in front of it.

## Connecting clients

```js
import { connect } from '@tursodatabase/sync';

const db = await connect({ path: 'local.db', url: 'https://sync.example.com', authToken: TOKEN });
await db.exec("INSERT INTO notes VALUES (1, 'hello')");
await db.push();
await db.pull();
```

```go
db, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
	Path:      "local.db",
	RemoteUrl: "https://sync.example.com",
	AuthToken: token,
})
```

### One database or many

- `turso-sync-server /data/main.db` serves one database; clients connect to the server root.
- `turso-sync-server --sync-dir /data/dbs` serves many: a client uses the URL `https://sync.example.com/db/{name}`, and the database is created on first access in `/data/dbs/{name}/data`. Names consist of lowercase Latin letters, digits, `-` and `_`.

### Endpoints

| Method and path | Purpose |
|---|---|
| `POST /v2/pipeline`, `POST /db/{name}/v2/pipeline` | Client SQL (the hrana protocol) |
| `POST /pull-updates`, `POST /db/{name}/pull-updates` | Changed pages for a client |
| `POST /admin/checkpoint`, `POST /db/{name}/admin/checkpoint` | Checkpoint now |
| `GET /health` | Liveness check, no token required |
| `GET /metrics` on `--metrics-listen` | Prometheus metrics, no token required |

The `turso-sync-server healthcheck` subcommand queries `/health`; the image's `HEALTHCHECK` uses it.

## Configuration

Every option has an environment variable; see `turso-sync-server --help` for the full list.

| Option | Variable | Default | What it does |
|---|---|---|---|
| `DATABASE` | `TURSO_SYNC_DB` | — | Database file (`:memory:` for an in-memory database) |
| `--listen` | `TURSO_SYNC_LISTEN` | `0.0.0.0:8080` | API address (alias `--sync-server`) |
| `--sync-dir` | `TURSO_SYNC_DIR` | — | Directory with many databases |
| `--auth-token` | `TURSO_SYNC_AUTH_TOKEN` | — | Access token |
| `--auth-token-file` | `TURSO_SYNC_AUTH_TOKEN_FILE` | — | Read the token from a file |
| `--request-timeout` | `TURSO_SYNC_REQUEST_TIMEOUT` | 60 s | Limit for receiving and processing a request, and for a client to keep reading a download |
| `--checkpoint-interval` | `TURSO_SYNC_CHECKPOINT_INTERVAL` | 300 s | How often to checkpoint (0 disables) |
| `--checkpoint-min-frames` | `TURSO_SYNC_CHECKPOINT_MIN_FRAMES` | 1000 | Skip a checkpoint while the WAL holds fewer frames |
| `--checkpoint-max-delay` | `TURSO_SYNC_CHECKPOINT_MAX_DELAY` | 300 s | How long downloads may hold a checkpoint off |
| `--keep-generations` | `TURSO_SYNC_KEEP_GENERATIONS` | 1000 | Generations to remember; older clients download the whole database |
| `--backup-url` | `TURSO_SYNC_BACKUP_URL` | — | `s3://bucket/prefix` or a directory for backups |
| `--backup-interval` | `TURSO_SYNC_BACKUP_INTERVAL` | 1 s | How often to upload new WAL frames |
| `--backup-snapshot-interval` | `TURSO_SYNC_BACKUP_SNAPSHOT_INTERVAL` | 86400 s | How often to take a full snapshot (0: at every checkpoint) |
| `--backup-retention-days` | `TURSO_SYNC_BACKUP_RETENTION_DAYS` | 0 | Days of history to keep (0 keeps everything) |
| `--metrics-listen` | `TURSO_SYNC_METRICS_LISTEN` | — | Address for `/metrics` |

## Checkpoints and generations

`tursodb --sync-server` never checkpoints: its WAL grows forever, because otherwise clients would lose changes. This server checkpoints every `--checkpoint-interval` once the WAL holds `--checkpoint-min-frames` frames. Before that it records which pages the generation changed, and a client from an older generation receives the union of those pages. The state lives next to the database in `{database}-sync/`. Backing it up along with the database is optional: without it, clients download the whole database once.

The server notices when the WAL was reset behind its back (a `PRAGMA wal_checkpoint` from a client, or the database edited with `sqlite3` while the server was down) and sends such clients the whole database instead of silently losing changes.

The server understands `tursodb` revisions, so clients switch over without downloading the database again.

Large downloads, such as a client's first one, are streamed in batches of 1 MB: memory use does not depend on the database size, and writes go on during the download. Checkpoints wait until the download ends; a client that stops reading for `--request-timeout` is disconnected. A first download split into parts (`pullBytesThreshold` in the JS client) holds checkpoints off for a minute after each part, so frequent ones would hold them off forever: after `--checkpoint-max-delay` the checkpoint goes ahead, and the downloads in progress fail (`connect()` rejects with status 410) and have to start over. Applications using `pullBytesThreshold` should retry `connect()`.

## Backups

With `--backup-url` the server backs up every generation into a directory of its own in the storage: a full snapshot of the database or a link to the previous generation, plus WAL frames uploaded in batches every `--backup-interval`. A full snapshot is taken on first start, after a WAL reset behind the server's back, and every `--backup-snapshot-interval`.

- **What a failure loses:** a crash loses up to `--backup-interval` of the latest changes. On a graceful shutdown (SIGTERM) the server uploads everything.
- **When S3 is unavailable:** checkpoints are postponed, the WAL grows, and uploads are retried with increasing pauses. Nothing is lost.
- **S3 access** uses the standard variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT` (for storage not run by AWS) and `AWS_ALLOW_HTTP=true` (for `http://`). The `object_store` library supports IAM roles as well, but the server has not been tested with them.
- **One prefix holds one database, written by one server.** Under `--sync-dir` every database gets its own prefix, `{url}/{name}`.
- **Cost:** a database under constant writes makes one S3 request per second. With many active databases, raise `--backup-interval`.
- **MVCC databases are not backed up.**

### Restore

If the database file is missing and the storage holds a backup, the server restores the database on startup (under `--sync-dir`, on first access to the database). Clients then download the whole database once.

By hand:

```sh
turso-sync-server backups --backup-url s3://my-bucket/turso/main          # list generations
turso-sync-server restore --backup-url s3://my-bucket/turso/main out.db    # latest state
turso-sync-server restore --backup-url s3://my-bucket/turso/main \
  --at 2026-10-17T12:00:00Z out.db                                          # state at a point in time
```

A restored file is a regular SQLite database and opens in `sqlite3`.

## Metrics

| Metric | Meaning |
|---|---|
| `turso_sync_http_requests_total{endpoint,status}` | Requests |
| `turso_sync_http_request_duration_seconds{endpoint}` | Response time |
| `turso_sync_wal_frames{db}` | Frames in the WAL of the current generation |
| `turso_sync_generation{db}` | Generation number |
| `turso_sync_checkpoints_total{db,outcome}` | Checkpoints: `done`, `deferred`, `failed` |
| `turso_sync_last_checkpoint_timestamp_seconds{db}` | Time of the last checkpoint |
| `turso_sync_backup_pending_frames{db}` | Frames not in the backup yet |
| `turso_sync_backup_snapshot_pending{db}` | 1 while the backup waits for a snapshot |
| `turso_sync_backup_synced_timestamp_seconds{db}` | When the backup last held every change |
| `turso_sync_backup_last_snapshot_timestamp_seconds{db}` | Time of the last snapshot |
| `turso_sync_backup_uploads_total{db,kind,result}` | Uploads (`snapshot`, `frames`; `ok`, `error`) |
| `turso_sync_backup_uploaded_bytes_total{db,kind,result}` | Bytes uploaded after compression |
| `turso_sync_open_databases` | Open databases |
| `process_resident_memory_bytes`, `process_cpu_seconds_total`, `process_open_fds` | Memory, CPU time and open descriptors of the server (memory and descriptors on Linux only) |

Example alerting rules:

```promql
# The backup lags by more than a minute
time() - turso_sync_backup_synced_timestamp_seconds > 60
# Uploads fail
increase(turso_sync_backup_uploads_total{result="error"}[10m]) > 0
# No checkpoint for a long time; the WAL keeps growing
time() - turso_sync_last_checkpoint_timestamp_seconds > 3600
```

## Limitations

- Partial sync is not supported: it does not work against a local server with `tursodb` either.
- MVCC databases behave as in `tursodb`: no checkpoints and no backups.

## Development

```sh
cargo build --release
cargo test
```

JS tests (`tests/js`, need Node 24):

```sh
cd tests/js && npm ci && npm test
# stress: file databases, backups and a checkpoint after every write;
# afterwards every database is restored from its backup and compared byte for byte
SYNC_SERVER_FILE_DB=1 SYNC_SERVER_BACKUP=1 SYNC_SERVER_ARGS=--checkpoint-after-write npm test -- sync.test.ts
# restore from real S3
BACKUP_S3_URL=s3://bucket/prefix AWS_ENDPOINT=… AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… npm test -- backup.test.ts
# clients switching over from tursodb
UPSTREAM_TURSODB=/path/to/tursodb npm test -- checkpoint.test.ts
# crashes: at every step of a checkpoint, a backup upload and a restore,
# then at random moments under load (10 times by default)
CRASH_ITERATIONS=300 npm test -- crash.test.ts
```

The crash tests stop the server at fixed points through `TURSO_SYNC_CRASH_AT=name[:n]`, which makes it kill itself the n-th time it gets there; the points are the `crash::point` calls in the source.

Go tests (`tests/go`):

```sh
cd tests/go
go test ./...                                         # this server's tests
./upstream.sh                                         # the driver's own sync tests
TURSO_SYNC_CHECKPOINT_AFTER_WRITE=true ./upstream.sh  # the same with a checkpoint after every write
```

Soak test (`tests/soak`): a pod meant to run for days with the server, Caddy in front of it, a load client and Prometheus. Writers, readers and fresh clients work through Caddy; every half hour writes pause and all clients must pull to the same state, and every three hours the backup is restored from S3 and checked against the acknowledged writes. Inconsistencies are counted in `soak_violations_total` and logged as `VIOLATION`.

```sh
tests/soak/deploy.sh   # builds the images and recreates the pod, keeping its data
```

The script reads `.env.podman` (the host that builds the server image) and `.env.scaleway` (the host that runs the pod, and the S3 keys) from the repository root. On that host Prometheus listens on `127.0.0.1:19090` and Caddy on `127.0.0.1:18000`; reach them through an SSH tunnel.

Tests on Linux in containers (`--format docker` keeps the `HEALTHCHECK`):

```sh
podman build --format docker -t localhost/turso-sync-server:dev .
podman build --format docker -t localhost/turso-sync-server-tests:dev tests/js
podman build --format docker -t localhost/turso-sync-server-go-tests:dev tests/go
podman run --rm localhost/turso-sync-server-tests:dev
podman run --rm localhost/turso-sync-server-go-tests:dev
```
