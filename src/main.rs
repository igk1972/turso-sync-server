mod generations;
mod http;
mod replica;
mod sync;

use std::io::{IsTerminal, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use turso_core::{DatabaseOpts, OpenFlags};

use crate::http::AppState;
use crate::replica::Target;
use crate::sync::{BackupConfig, CheckpointConfig, OpenConfig, TursoSyncServer};

/// Covers the upload of the WAL tail written since the last backup tick.
const SHUTDOWN_BACKUP_TIMEOUT: Duration = Duration::from_secs(25);

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about = "Turso sync protocol server", subcommand_negates_reqs = true)]
struct Opts {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(
        env = "TURSO_SYNC_DB",
        required_unless_present = "sync_dir",
        conflicts_with = "sync_dir",
        help = "Database file to serve (\":memory:\" for an in-memory database)"
    )]
    database: Option<String>,
    #[arg(
        long,
        global = true,
        visible_alias = "sync-server",
        env = "TURSO_SYNC_LISTEN",
        default_value = "0.0.0.0:8080"
    )]
    listen: String,
    #[arg(
        long,
        env = "TURSO_SYNC_DIR",
        help = "Serve every database under PATH, addressed as /db/{name}"
    )]
    sync_dir: Option<PathBuf>,
    #[arg(
        long,
        env = "TURSO_SYNC_MAX_DATABASES",
        default_value_t = 256,
        help = "Most databases held open at once under --sync-dir"
    )]
    sync_max_databases: usize,
    #[arg(
        long,
        env = "TURSO_SYNC_AUTH_TOKEN",
        hide_env_values = true,
        help = "Require `Authorization: Bearer <token>` on sync endpoints"
    )]
    auth_token: Option<String>,
    #[arg(
        long,
        env = "TURSO_SYNC_AUTH_TOKEN_FILE",
        conflicts_with = "auth_token",
        help = "Read the auth token from a file"
    )]
    auth_token_file: Option<PathBuf>,
    #[arg(long, env = "TURSO_SYNC_MAX_REQUEST_BYTES", default_value_t = 128 * 1024 * 1024)]
    max_request_bytes: usize,
    #[arg(
        long,
        env = "TURSO_SYNC_REQUEST_TIMEOUT",
        default_value_t = 60,
        help = "Seconds allowed to receive and process one request"
    )]
    request_timeout: u64,
    #[arg(
        long,
        env = "TURSO_SYNC_CHECKPOINT_INTERVAL",
        default_value_t = 300,
        help = "Seconds between checkpoints of a database (0 disables periodic checkpoints)"
    )]
    checkpoint_interval: u64,
    #[arg(
        long,
        env = "TURSO_SYNC_CHECKPOINT_MIN_FRAMES",
        default_value_t = 1000,
        help = "Skip a periodic checkpoint while the WAL holds fewer frames"
    )]
    checkpoint_min_frames: u64,
    #[arg(
        long,
        env = "TURSO_SYNC_KEEP_GENERATIONS",
        default_value_t = 1000,
        help = "Checkpoint generations to remember; older clients download the whole database"
    )]
    keep_generations: usize,
    /// Checkpoint after every write request (for testing).
    #[arg(long, hide = true, env = "TURSO_SYNC_CHECKPOINT_AFTER_WRITE")]
    checkpoint_after_write: bool,
    #[arg(
        long,
        global = true,
        env = "TURSO_SYNC_BACKUP_URL",
        help = "Back up to s3://bucket/prefix (configured through AWS_* variables) or to a directory"
    )]
    backup_url: Option<String>,
    #[arg(
        long,
        env = "TURSO_SYNC_BACKUP_INTERVAL",
        default_value_t = 1,
        help = "Seconds between uploads of new WAL frames"
    )]
    backup_interval: u64,
    #[arg(
        long,
        env = "TURSO_SYNC_BACKUP_SNAPSHOT_INTERVAL",
        default_value_t = 86400,
        help = "Seconds between full snapshots (0 takes one at every checkpoint)"
    )]
    backup_snapshot_interval: u64,
    #[arg(
        long,
        env = "TURSO_SYNC_BACKUP_RETENTION_DAYS",
        default_value_t = 0,
        help = "Days of backup history to keep (0 keeps everything)"
    )]
    backup_retention_days: u64,
    #[arg(long, env = "TURSO_SYNC_VFS")]
    vfs: Option<String>,
    #[arg(long)]
    readonly: bool,
    #[arg(long)]
    experimental_views: bool,
    #[arg(long)]
    experimental_custom_types: bool,
    #[arg(long)]
    experimental_encryption: bool,
    #[arg(long)]
    experimental_index_method: bool,
    #[arg(long)]
    experimental_autovacuum: bool,
    #[arg(long)]
    experimental_vacuum: bool,
    #[arg(long)]
    experimental_attach: bool,
    #[arg(long)]
    experimental_generated_columns: bool,
    #[arg(long)]
    experimental_without_rowid: bool,
    #[arg(long)]
    experimental_mvcc_passive_checkpoint: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Exit successfully if the server listening on --listen answers /health
    Healthcheck,
    /// Restore a database from --backup-url
    Restore {
        /// Where to write the database
        output: PathBuf,
        /// Restore the state at this time (RFC 3339, e.g. 2026-10-17T12:00:00Z)
        #[arg(long)]
        at: Option<String>,
        /// Replace an existing database, its WAL and sync state
        #[arg(long)]
        force: bool,
    },
    /// List the backup generations stored at --backup-url
    Backups,
}

impl Opts {
    fn backup_config(&self) -> Result<Option<BackupConfig>> {
        let Some(url) = &self.backup_url else {
            return Ok(None);
        };
        if self.readonly {
            bail!("--backup-url needs a writable database");
        }
        if self.database.as_deref() == Some(":memory:") {
            bail!("--backup-url needs a database file");
        }
        Ok(Some(BackupConfig {
            target: Target::open(url)?,
            interval: Duration::from_secs(self.backup_interval.max(1)),
            snapshot_interval: Duration::from_secs(self.backup_snapshot_interval),
            retention: (self.backup_retention_days > 0)
                .then(|| Duration::from_secs(self.backup_retention_days * 24 * 3600)),
        }))
    }

    fn backup_target(&self) -> Result<Arc<Target>> {
        let url = self
            .backup_url
            .as_deref()
            .ok_or_else(|| anyhow!("--backup-url is required"))?;
        Target::open(url)
    }

    fn open_config(&self, backup: Option<BackupConfig>) -> OpenConfig {
        let mut flags = OpenFlags::default();
        flags.set(OpenFlags::ReadOnly, self.readonly);
        OpenConfig {
            vfs: self.vfs.clone(),
            flags,
            db_opts: DatabaseOpts::new()
                .with_views(self.experimental_views)
                .with_custom_types(self.experimental_custom_types)
                .with_encryption(self.experimental_encryption)
                .with_index_method(self.experimental_index_method)
                .with_autovacuum(self.experimental_autovacuum)
                .with_vacuum(self.experimental_vacuum)
                .with_attach(self.experimental_attach)
                .with_generated_columns(self.experimental_generated_columns)
                .with_without_rowid(self.experimental_without_rowid)
                .with_experimental_mvcc_passive_checkpoint(
                    self.experimental_mvcc_passive_checkpoint,
                ),
            max_open: self.sync_max_databases,
            keep_generations: self.keep_generations.max(1),
            backup,
        }
    }

    fn checkpoint_config(&self) -> CheckpointConfig {
        CheckpointConfig {
            interval: (self.checkpoint_interval > 0)
                .then(|| Duration::from_secs(self.checkpoint_interval)),
            min_frames: self.checkpoint_min_frames,
            after_write: self.checkpoint_after_write,
        }
    }

    fn auth_token(&self) -> Result<Option<String>> {
        let token = match (&self.auth_token, &self.auth_token_file) {
            (Some(token), _) => token.trim().to_string(),
            (None, Some(path)) => std::fs::read_to_string(path)
                .with_context(|| format!("failed to read auth token file {}", path.display()))?
                .trim()
                .to_string(),
            (None, None) => return Ok(None),
        };
        if token.is_empty() {
            return Err(anyhow!("auth token is empty"));
        }
        Ok(Some(token))
    }
}

fn healthcheck(listen: &str) -> Result<()> {
    let mut addr = listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("cannot resolve {listen}"))?;
    if addr.ip().is_unspecified() {
        let loopback = match addr {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        };
        addr.set_ip(loopback);
    }
    let timeout = Duration::from_secs(3);
    let mut stream = TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.write_all(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    match response.split_whitespace().nth(1) {
        Some("200") => Ok(()),
        status => Err(anyhow!("unhealthy: status {}", status.unwrap_or("missing"))),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,turso_sync_server=info")),
        )
        .init();

    let opts = Opts::parse();
    match &opts.command {
        Some(Command::Healthcheck) => return healthcheck(&opts.listen),
        Some(Command::Restore { output, at, force }) => {
            return restore(opts.backup_target()?.as_ref(), output, at.as_deref(), *force).await
        }
        Some(Command::Backups) => return list_backups(opts.backup_target()?.as_ref()).await,
        None => {}
    }
    let auth_token = opts.auth_token()?;
    let backup = opts.backup_config()?;
    let backups_enabled = backup.is_some();
    let config = opts.open_config(backup);
    let checkpoint = opts.checkpoint_config();

    let server = match (&opts.sync_dir, &opts.database) {
        (Some(dir), _) => {
            info!("serving databases under {}", dir.display());
            TursoSyncServer::new_dir(dir.clone(), config, checkpoint)?
        }
        (None, Some(path)) => {
            info!("serving database {path}");
            if let Some(backup) = &config.backup {
                replica::restore_missing(&backup.target, Path::new(path)).await?;
            }
            TursoSyncServer::new(path, &config, checkpoint)?
        }
        (None, None) => return Err(anyhow!("a database path or --sync-dir is required")),
    };
    if auth_token.is_none() {
        warn!("no auth token configured: anyone who can reach the server can read and write");
    }

    let state = Arc::new(AppState {
        server,
        auth_token,
        request_timeout: Duration::from_secs(opts.request_timeout),
    });
    if let Some(interval) = checkpoint.interval {
        let state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval.min(Duration::from_secs(10)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let state = state.clone();
                let _ = tokio::task::spawn_blocking(move || state.server.run_due_checkpoints()).await;
            }
        });
    }
    let app = http::router(state.clone(), opts.max_request_bytes);
    let listener = tokio::net::TcpListener::bind(&opts.listen)
        .await
        .with_context(|| format!("failed to listen on {}", opts.listen))?;
    info!("listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    if backups_enabled {
        info!("uploading the rest of the WAL to the backup");
        let flush = tokio::task::spawn_blocking(move || state.server.flush_backups());
        if tokio::time::timeout(SHUTDOWN_BACKUP_TIMEOUT, flush).await.is_err() {
            warn!("gave up on the final backup upload");
        }
    }
    info!("stopped");
    Ok(())
}

async fn restore(target: &Target, output: &Path, at: Option<&str>, force: bool) -> Result<()> {
    if output.exists() && !force {
        bail!("{} exists; pass --force to replace it", output.display());
    }
    let at = at
        .map(|at| DateTime::parse_from_rfc3339(at).map(|time| time.with_timezone(&Utc)))
        .transpose()
        .context("--at expects an RFC 3339 time such as 2026-10-17T12:00:00Z")?;
    let Some(restored) = replica::restore(target, output, at).await? else {
        match at {
            Some(at) => bail!("{target} holds no backup from before {at}"),
            None => bail!("{target} holds no backup"),
        }
    };
    let tip = restored.chain.last().expect("a restore replays at least one generation");
    println!(
        "restored {} to generation {tip}: {} generations, {} frames replayed",
        output.display(),
        restored.chain.len(),
        restored.frames,
    );
    if !restored.complete {
        println!("the backup has a gap, so later changes could not be restored");
    }
    Ok(())
}

async fn list_backups(target: &Target) -> Result<()> {
    let format_time = |time: Option<DateTime<Utc>>| {
        time.map_or("-".to_string(), |time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
    };
    println!("{:<34} {:<20} {:<20} {:>10}  BASE", "GENERATION", "STARTED", "LAST UPLOAD", "FRAMES");
    for generation in replica::list(target).await? {
        let base = match &generation.meta {
            Some(meta) => meta.to_string(),
            None => "incomplete".to_string(),
        };
        println!(
            "{:<34} {:<20} {:<20} {:>10}  {base}",
            generation.id,
            format_time(replica::generation_time(&generation.id)),
            format_time(generation.last_upload),
            generation.last_frame,
        );
    }
    Ok(())
}

async fn shutdown_signal() {
    let mut terminate = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
    info!("shutdown signal received");
}
