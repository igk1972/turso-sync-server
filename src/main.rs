mod http;
mod sync;

use std::io::{IsTerminal, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use turso_core::{DatabaseOpts, OpenFlags};

use crate::http::AppState;
use crate::sync::{OpenConfig, TursoSyncServer};

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
}

impl Opts {
    fn open_config(&self) -> OpenConfig {
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
    if let Some(Command::Healthcheck) = opts.command {
        return healthcheck(&opts.listen);
    }
    let auth_token = opts.auth_token()?;
    let config = opts.open_config();

    let server = match (&opts.sync_dir, &opts.database) {
        (Some(dir), _) => {
            info!("serving databases under {}", dir.display());
            TursoSyncServer::new_dir(dir.clone(), config)?
        }
        (None, Some(path)) => {
            info!("serving database {path}");
            TursoSyncServer::new(path, &config)?
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
    let app = http::router(state, opts.max_request_bytes);
    let listener = tokio::net::TcpListener::bind(&opts.listen)
        .await
        .with_context(|| format!("failed to listen on {}", opts.listen))?;
    info!("listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("stopped");
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
