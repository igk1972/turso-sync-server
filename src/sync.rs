use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use prost::Message;
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use turso_core::{
    CheckpointMode, Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect,
    Value as CoreValue,
};

use crate::crash;
use crate::generations::{db_file_meta, Generations, Revision, SealedPages};
use crate::metrics::{self, metrics, CheckpointLabels};
use crate::replica::{self, Job, Replica, Target};
use turso_sync_engine::server_proto::{
    BatchCond, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp, Col, Error,
    ExecuteStreamReq, ExecuteStreamResp, MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto,
    PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody,
    PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
    PullUpdatesStreamKind, Row, StmtResult, StreamRequest, StreamResponse, StreamResult, Value,
};

const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;
const MVCC_LOG_MAGIC: u32 = 0x4C4D4C32;
const MVCC_LOG_VERSION: u8 = 3;
const MVCC_LOG_HEADER_SIZE: usize = 56;
const MVCC_LOG_HEADER_SALT_START: usize = 8;
const MVCC_LOG_HEADER_SALT_END: usize = 16;
const MVCC_LOG_HEADER_RESERVED_START: usize = 16;
const MVCC_LOG_HEADER_CRC_START: usize = 52;
const MVCC_TX_FRAME_MAGIC: u32 = 0x5854564D;
const MVCC_TX_EXT_FRAME_MAGIC: u32 = 0x5845564D;
const MVCC_TX_END_MAGIC: u32 = 0x4554564D;
const MVCC_TX_HEADER_SIZE: usize = 24;
const MVCC_TX_EXT_HEADER_SIZE: usize = 40;
const MVCC_TX_TRAILER_SIZE: usize = 8;
const MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK: u32 = 1 << 0;
/// Chunked bootstraps pin later chunks to the revision of the first one, and a
/// checkpoint in between would make that revision unreadable. Pinned reads and
/// page-selector reads therefore hold checkpoints off for this long.
const PINNED_READ_LEASE: Duration = Duration::from_secs(60);
/// Page sets larger than one batch are streamed batch by batch.
const DOWNLOAD_BATCH_PAGES: usize = 256;
const DOWNLOAD_QUEUE_BATCHES: usize = 4;
/// A client that stops reading a download would hold checkpoints off for good.
const DEFAULT_DOWNLOAD_STALL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a checkpoint waits for a backup upload running without the lock.
const BACKUP_IDLE_WAIT: Duration = Duration::from_secs(30);
const BACKUP_MAX_BACKOFF: Duration = Duration::from_secs(60);
/// How long one scrape waits in total for busy databases.
const METRICS_LOCK_WAIT: Duration = Duration::from_millis(500);

pub struct OpenConfig {
    pub vfs: Option<String>,
    pub flags: OpenFlags,
    pub db_opts: DatabaseOpts,
    pub max_open: usize,
    pub keep_generations: usize,
    pub backup: Option<BackupConfig>,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointConfig {
    pub interval: Option<Duration>,
    pub min_frames: u64,
    pub after_write: bool,
}

#[derive(Clone)]
pub struct BackupConfig {
    pub target: Arc<Target>,
    pub interval: Duration,
    pub snapshot_interval: Duration,
    pub retention: Option<Duration>,
}

pub(crate) struct DbHandle {
    state: Mutex<DbState>,
    /// Signalled when a backup upload that ran without the lock finishes.
    backup_idle: Condvar,
    path: String,
}

impl DbHandle {
    /// Locks the database once no backup upload is in flight, so that a
    /// checkpoint does not change the files under it.
    fn lock_idle(&self) -> MutexGuard<'_, DbState> {
        let state = self.state.lock().unwrap();
        self.backup_idle
            .wait_timeout_while(state, BACKUP_IDLE_WAIT, |state| {
                state.replica.as_ref().is_some_and(Replica::busy)
            })
            .unwrap()
            .0
    }

    fn finish_backup_job(&self, job: &Job, succeeded: bool) -> Result<bool> {
        let result = self.state.lock().unwrap().finish_backup_job(job, succeeded);
        self.backup_idle.notify_all();
        result
    }
}

struct DbState {
    conn: Arc<Connection>,
    /// `None` for MVCC databases, which keep the upstream single-generation behaviour.
    generations: Option<Generations>,
    checkpoint_seq: u32,
    wal_salts_recorded: bool,
    pinned_until: Option<Instant>,
    last_checkpoint: Instant,
    last_checkpoint_at: Option<SystemTime>,
    replica: Option<Replica>,
    active_downloads: Arc<AtomicUsize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CheckpointOutcome {
    Done {
        generation: u64,
        frames: u64,
        pages: u64,
    },
    Deferred,
    Skipped,
}

impl DbState {
    fn checkpoint(&mut self, path: &str, min_frames: u64) -> Result<CheckpointOutcome> {
        let result = self.try_checkpoint(path, min_frames);
        let outcome = match &result {
            Ok(CheckpointOutcome::Done { .. }) => "done",
            Ok(CheckpointOutcome::Deferred) => "deferred",
            Ok(CheckpointOutcome::Skipped) => return result,
            Err(_) => "failed",
        };
        let labels = CheckpointLabels {
            db: path.to_string(),
            outcome,
        };
        metrics().checkpoints.get_or_create(&labels).inc();
        result
    }

    /// Seals the current generation and truncates the WAL.
    fn try_checkpoint(&mut self, path: &str, min_frames: u64) -> Result<CheckpointOutcome> {
        let Some(generations) = self.generations.as_mut() else {
            return Ok(CheckpointOutcome::Skipped);
        };
        if self.pinned_until.is_some_and(|until| Instant::now() < until)
            || self.active_downloads.load(Ordering::SeqCst) > 0
            || self.replica.as_ref().is_some_and(Replica::busy)
        {
            return Ok(CheckpointOutcome::Deferred);
        }
        let wal = self.conn.wal_state()?;
        if wal.max_frame == 0 || wal.max_frame < min_frames {
            return Ok(CheckpointOutcome::Skipped);
        }
        // The backup has to hold the whole WAL before the checkpoint drops it;
        // otherwise the next backup generation has to start from a snapshot.
        let backed_up = match self.replica.as_mut() {
            Some(replica) => {
                let (epoch, current) = (generations.epoch(), generations.current());
                match replica.flush_blocking(&self.conn, epoch, current, false) {
                    Ok(backed_up) => backed_up,
                    Err(err) => {
                        warn!("{path}: checkpoint postponed, the backup upload failed: {err:#}");
                        return Ok(CheckpointOutcome::Deferred);
                    }
                }
            }
            None => false,
        };
        let pages: RoaringBitmap = self.conn.wal_changed_pages_after(0)?.into_iter().collect();
        let page_count = pages.len();
        generations.seal(SealedPages::Known(pages))?;
        crash::point("checkpoint.sealed");
        // The bound makes the checkpoint fail rather than fold frames that are
        // missing from the sealed page set. A failure leaves the WAL in place,
        // which is safe: the next generation then repeats those pages.
        let result = self.conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: Some(wal.max_frame),
        });
        crash::point("checkpoint.truncated");
        let after = self.conn.wal_state()?;
        self.checkpoint_seq = after.checkpoint_seq_no;
        generations.store_identity(after.checkpoint_seq_no, after.max_frame > 0)?;
        crash::point("checkpoint.identity-stored");
        self.wal_salts_recorded = after.max_frame > 0;
        self.last_checkpoint = Instant::now();
        self.last_checkpoint_at = Some(SystemTime::now());
        if let Some(replica) = self.replica.as_mut() {
            replica.on_checkpoint(generations.epoch(), generations.current(), backed_up)?;
        }
        if let Err(err) = result {
            return Err(anyhow!("checkpoint failed, the WAL is kept: {err}"));
        }
        info!(
            "{path}: checkpointed {} frames ({page_count} pages), generation {}",
            wal.max_frame,
            generations.current()
        );
        Ok(CheckpointOutcome::Done {
            generation: generations.current(),
            frames: wal.max_frame,
            pages: page_count,
        })
    }

    /// Tracks the WAL after a write request: a reset that did not come from
    /// [`DbState::checkpoint`] loses the pages it folded, so the generation is
    /// sealed as unknown and older clients download everything again.
    fn after_write(&mut self, path: &str, config: &CheckpointConfig) -> Result<()> {
        let Some(generations) = self.generations.as_mut() else {
            return Ok(());
        };
        let wal = self.conn.wal_state()?;
        if wal.checkpoint_seq_no != self.checkpoint_seq {
            warn!("{path}: a request reset the WAL; clients will download the database again");
            generations.seal(SealedPages::Unknown)?;
            self.checkpoint_seq = wal.checkpoint_seq_no;
            generations.store_identity(wal.checkpoint_seq_no, wal.max_frame > 0)?;
            self.wal_salts_recorded = wal.max_frame > 0;
        } else if wal.max_frame > 0 && !self.wal_salts_recorded {
            generations.store_identity(wal.checkpoint_seq_no, true)?;
            self.wal_salts_recorded = true;
        }
        if config.after_write {
            self.checkpoint(path, 1)?;
        }
        Ok(())
    }

    fn backup_job(&mut self) -> Result<Option<Job>> {
        let (Some(replica), Some(generations)) = (self.replica.as_mut(), self.generations.as_ref())
        else {
            return Ok(None);
        };
        let wal = self.conn.wal_state()?;
        replica.next_job(&self.conn, generations.epoch(), generations.current(), &wal, true)
    }

    fn finish_backup_job(&mut self, job: &Job, succeeded: bool) -> Result<bool> {
        let Some(replica) = self.replica.as_mut() else {
            return Ok(false);
        };
        match self.conn.wal_state() {
            Ok(wal) => replica.finish(job, succeeded, &wal),
            Err(err) => {
                replica.abandon();
                Err(err.into())
            }
        }
    }

    fn refresh_metrics(&self, path: &str) -> Result<()> {
        let m = metrics();
        let labels = metrics::db(path);
        let wal = self.conn.wal_state()?;
        m.wal_frames.get_or_create(&labels).set(wal.max_frame as i64);
        if let Some(generations) = &self.generations {
            m.generation.get_or_create(&labels).set(generations.current() as i64);
        }
        if let Some(at) = self.last_checkpoint_at {
            m.last_checkpoint.get_or_create(&labels).set(metrics::unix_seconds(at));
        }
        if let Some(replica) = &self.replica {
            let pending = replica.pending_frames(wal.max_frame);
            m.backup_pending_frames.get_or_create(&labels).set(pending as i64);
            let snapshot_pending = i64::from(replica.snapshot_pending());
            m.backup_snapshot_pending.get_or_create(&labels).set(snapshot_pending);
            if let Some(at) = replica.synced_at() {
                m.backup_synced.get_or_create(&labels).set(metrics::unix_seconds(at));
            }
            if let Some(at) = replica.snapshot_at() {
                m.backup_last_snapshot.get_or_create(&labels).set(metrics::unix_seconds(at));
            }
        }
        Ok(())
    }

    /// Uploads what the backup lacks, a snapshot included.
    fn flush_backup(&mut self) -> Result<()> {
        let (Some(replica), Some(generations)) = (self.replica.as_mut(), self.generations.as_ref())
        else {
            return Ok(());
        };
        replica.flush_blocking(&self.conn, generations.epoch(), generations.current(), true)?;
        Ok(())
    }
}

struct OpenHandles {
    handles: HashMap<String, Arc<DbHandle>>,
    capacity: usize,
}

enum DbSource {
    Single(Arc<DbHandle>),
    Dir {
        base: PathBuf,
        config: Box<OpenConfig>,
        open_handles: Mutex<OpenHandles>,
    },
}

pub struct TursoSyncServer {
    source: DbSource,
    checkpoint: CheckpointConfig,
    download_stall_timeout: Duration,
}

impl TursoSyncServer {
    pub fn new(db_path: &str, config: &OpenConfig, checkpoint: CheckpointConfig) -> Result<Self> {
        Ok(Self {
            source: DbSource::Single(open_db_handle(
                Path::new(db_path),
                config,
                config.backup.as_ref().map(|backup| backup.target.clone()),
            )?),
            checkpoint,
            download_stall_timeout: DEFAULT_DOWNLOAD_STALL_TIMEOUT,
        })
    }

    pub fn new_dir(
        base: PathBuf,
        config: OpenConfig,
        checkpoint: CheckpointConfig,
    ) -> Result<Self> {
        if !base.is_dir() {
            return Err(anyhow!(
                "sync dir path does not exist or is not a directory: {}",
                base.display()
            ));
        }
        let open_handles = Mutex::new(OpenHandles::new(config.max_open));
        Ok(Self {
            source: DbSource::Dir {
                base: base.canonicalize()?,
                config: Box::new(config),
                open_handles,
            },
            checkpoint,
            download_stall_timeout: DEFAULT_DOWNLOAD_STALL_TIMEOUT,
        })
    }

    /// How long a download waits for a client that stopped reading.
    pub fn with_download_stall_timeout(mut self, timeout: Duration) -> Self {
        self.download_stall_timeout = timeout;
        self
    }

    fn open_databases(&self) -> Vec<Arc<DbHandle>> {
        match &self.source {
            DbSource::Single(handle) => vec![handle.clone()],
            DbSource::Dir { open_handles, .. } => {
                open_handles.lock().unwrap().handles.values().cloned().collect()
            }
        }
    }

    /// Checkpoints every open database whose last checkpoint is older than the
    /// configured interval and whose WAL holds enough frames.
    pub fn run_due_checkpoints(&self) {
        let Some(interval) = self.checkpoint.interval else {
            return;
        };
        for handle in self.open_databases() {
            if handle.state.lock().unwrap().last_checkpoint.elapsed() < interval {
                continue;
            }
            let mut state = handle.lock_idle();
            if let Err(err) = state.checkpoint(&handle.path, self.checkpoint.min_frames) {
                error!("{}: checkpoint failed: {err}", handle.path);
            }
        }
    }

    /// Updates the database gauges. Locks are held briefly most of the time;
    /// a database busy past the deadline keeps its previous values rather
    /// than holding up the scrape.
    pub fn refresh_metrics(&self) {
        let databases = self.open_databases();
        metrics().open_databases.set(databases.len() as i64);
        let deadline = Instant::now() + METRICS_LOCK_WAIT;
        for handle in databases {
            let state = loop {
                match handle.state.try_lock() {
                    Ok(state) => break Some(state),
                    Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break None,
                }
            };
            let Some(state) = state else {
                continue;
            };
            if let Err(err) = state.refresh_metrics(&handle.path) {
                warn!("{}: failed to read metrics: {err}", handle.path);
            }
        }
    }

    /// Uploads whatever the backups lack. Meant for shutdown.
    pub fn flush_backups(&self) {
        for handle in self.open_databases() {
            if let Err(err) = handle.lock_idle().flush_backup() {
                error!("{}: final backup upload failed: {err:#}", handle.path);
            }
        }
    }

    pub(crate) fn handle_checkpoint(&self, db: &DbHandle) -> Result<HttpResponse> {
        let outcome = db.lock_idle().checkpoint(&db.path, 1)?;
        let body = match outcome {
            CheckpointOutcome::Done {
                generation,
                frames,
                pages,
            } => serde_json::json!({
                "status": "done",
                "generation": generation,
                "frames": frames,
                "pages": pages,
            }),
            CheckpointOutcome::Deferred => serde_json::json!({ "status": "deferred" }),
            CheckpointOutcome::Skipped => serde_json::json!({ "status": "skipped" }),
        };
        Ok(HttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body: serde_json::to_vec(&body)?,
            stream: None,
        })
    }

    pub(crate) fn resolve_db(
        &self,
        requested: Option<&str>,
    ) -> std::result::Result<Arc<DbHandle>, HttpResponse> {
        match (&self.source, requested) {
            (DbSource::Single(h), None) => Ok(h.clone()),
            (DbSource::Single(_), Some(_)) => Err(text_response(404, "Not Found")),
            (DbSource::Dir { .. }, None) => Err(text_response(404, "Not Found")),
            (
                DbSource::Dir {
                    base,
                    config,
                    open_handles,
                },
                Some(name),
            ) => {
                if !validate_db_name(name) {
                    return Err(text_response(400, "Invalid database name"));
                }
                let mut open_handles = open_handles.lock().unwrap();
                open_handles.get_or_open(name, || {
                    let path = db_path_for(base, name);
                    let dir = path.parent().expect("database path has a parent directory");
                    if !config.flags.contains(OpenFlags::ReadOnly) {
                        if let Err(err) = std::fs::create_dir_all(dir) {
                            error!("failed to create directory for database {name}: {err}");
                            return Err(text_response(
                                500,
                                &format!("Internal Server Error: {err}"),
                            ));
                        }
                    }
                    if !dir.canonicalize().is_ok_and(|dir| dir.starts_with(base)) {
                        return Err(text_response(404, "Not Found"));
                    }
                    let backup = config.backup.as_ref().map(|backup| backup.target.child(name));
                    if let Some(target) = &backup {
                        let restored = target
                            .runtime()
                            .block_on(replica::restore_missing(target, &path));
                        if let Err(err) = restored {
                            error!("failed to restore database {name}: {err:#}");
                            return Err(text_response(503, "Service Unavailable"));
                        }
                    }
                    match open_db_handle(&path, config, backup) {
                        Ok(handle) => Ok(handle),
                        // Sync clients retry a 500 forever but can act on a 404.
                        Err(err) if path.exists() => {
                            error!("failed to open database {name}: {err}");
                            Err(text_response(500, &format!("Internal Server Error: {err}")))
                        }
                        Err(err) => {
                            debug!("no database named {name}: {err}");
                            Err(text_response(404, "Not Found"))
                        }
                    }
                })
            }
        }
    }

    pub(crate) fn handle_pipeline(&self, db: &DbHandle, body: &[u8]) -> Result<HttpResponse> {
        let req: PipelineReqBody = serde_json::from_slice(body)
            .map_err(|e| anyhow!("Failed to parse pipeline request: {}", e))?;

        debug!("Pipeline request: {:?}", req);

        let mut state = if self.checkpoint.after_write {
            db.lock_idle()
        } else {
            db.state.lock().unwrap()
        };
        let conn = state.conn.clone();

        let mut results = Vec::new();

        for request in req.requests {
            let result = match request {
                StreamRequest::Execute(exec_req) => self.execute_statement(&conn, &exec_req),
                StreamRequest::Batch(batch_req) => self.execute_batch(&conn, &batch_req),
                StreamRequest::None => StreamResult::Error {
                    error: Error {
                        message: "Unknown request type".to_string(),
                        code: "UNKNOWN".to_string(),
                    },
                },
            };
            results.push(result);
        }

        // Batons are not supported and every client shares this connection, so a
        // transaction left open by a failed batch would swallow other clients' writes.
        if !conn.get_auto_commit() {
            warn!("pipeline request left a transaction open, rolling it back");
            if let Err(err) = conn.execute("ROLLBACK") {
                error!("failed to roll back a dangling transaction: {err}");
            }
        }
        state.after_write(&db.path, &self.checkpoint)?;

        let resp = PipelineRespBody {
            baton: req.baton,
            base_url: None,
            results,
        };

        let body = serde_json::to_vec(&resp)?;

        Ok(HttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body,
            stream: None,
        })
    }

    fn execute_statement(&self, conn: &Arc<Connection>, req: &ExecuteStreamReq) -> StreamResult {
        let sql = match &req.stmt.sql {
            Some(s) => s.clone(),
            None => {
                return StreamResult::Error {
                    error: Error {
                        message: "No SQL provided".to_string(),
                        code: "NO_SQL".to_string(),
                    },
                }
            }
        };

        debug!("Executing SQL: {}", sql);

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                // Errors in client SQL go back in the response and are not
                // server failures; sync clients probe missing tables routinely.
                debug!("Failed to prepare statement: {}", e);
                return StreamResult::Error {
                    error: Error {
                        message: e.to_string(),
                        code: "PREPARE_ERROR".to_string(),
                    },
                };
            }
        };

        for (i, arg) in req.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            if let Err(err) = stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value) {
                debug!("Failed to bind statement argument: {}", err);
                return StreamResult::Error {
                    error: Error {
                        message: err.to_string(),
                        code: "BIND_ERROR".to_string(),
                    },
                };
            }
        }

        let want_rows = req.stmt.want_rows.unwrap_or(true);

        if want_rows {
            match stmt.run_collect_rows() {
                Ok(rows) => {
                    let cols: Vec<Col> = (0..stmt.num_columns())
                        .map(|i| Col {
                            name: Some(stmt.get_column_name(i).to_string()),
                            decltype: stmt.get_column_decltype(i),
                        })
                        .collect();

                    let result_rows: Vec<Row> = rows
                        .into_iter()
                        .map(|row| Row {
                            values: row.into_iter().map(convert_core_to_value).collect(),
                        })
                        .collect();

                    StreamResult::Ok {
                        response: StreamResponse::Execute(ExecuteStreamResp {
                            result: StmtResult {
                                cols,
                                rows: result_rows,
                                affected_row_count: 0,
                                last_insert_rowid: None,
                                replication_index: None,
                                rows_read: 0,
                                rows_written: 0,
                                query_duration_ms: 0.0,
                            },
                        }),
                    }
                }
                Err(e) => {
                    debug!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        } else {
            match stmt.run_ignore_rows() {
                Ok(()) => StreamResult::Ok {
                    response: StreamResponse::Execute(ExecuteStreamResp {
                        result: StmtResult {
                            cols: vec![],
                            rows: vec![],
                            affected_row_count: 0,
                            last_insert_rowid: None,
                            replication_index: None,
                            rows_read: 0,
                            rows_written: 0,
                            query_duration_ms: 0.0,
                        },
                    }),
                },
                Err(e) => {
                    debug!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        }
    }

    fn execute_batch(&self, conn: &Arc<Connection>, req: &BatchStreamReq) -> StreamResult {
        let batch = &req.batch;
        let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
        let mut step_errors: Vec<Option<Error>> = Vec::with_capacity(batch.steps.len());

        for (step_idx, step) in batch.steps.iter().enumerate() {
            let should_execute = match &step.condition {
                None => true,
                Some(cond) => Self::evaluate_condition(cond, &step_results, &step_errors, conn),
            };

            if should_execute {
                let result = self.execute_batch_step(conn, step);
                match result {
                    Ok(stmt_result) => {
                        step_results.push(Some(stmt_result));
                        step_errors.push(None);
                    }
                    Err(e) => {
                        debug!("Batch step {} failed: {}", step_idx, e);
                        step_results.push(None);
                        step_errors.push(Some(Error {
                            message: e.to_string(),
                            code: "BATCH_STEP_ERROR".to_string(),
                        }));
                    }
                }
            } else {
                step_results.push(None);
                step_errors.push(None);
            }
        }

        StreamResult::Ok {
            response: StreamResponse::Batch(BatchStreamResp {
                result: BatchResult {
                    step_results,
                    step_errors,
                    replication_index: None,
                },
            }),
        }
    }

    fn evaluate_condition(
        cond: &BatchCond,
        step_results: &[Option<StmtResult>],
        step_errors: &[Option<Error>],
        conn: &Arc<Connection>,
    ) -> bool {
        match cond {
            BatchCond::None => true,
            BatchCond::Ok { step } => {
                let idx = *step as usize;
                idx < step_results.len() && step_results[idx].is_some()
            }
            BatchCond::Error { step } => {
                let idx = *step as usize;
                idx < step_errors.len() && step_errors[idx].is_some()
            }
            BatchCond::Not { cond } => {
                !Self::evaluate_condition(cond, step_results, step_errors, conn)
            }
            BatchCond::And(list) => list
                .conds
                .iter()
                .all(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::Or(list) => list
                .conds
                .iter()
                .any(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::IsAutocommit {} => conn.get_auto_commit(),
        }
    }

    fn execute_batch_step(&self, conn: &Arc<Connection>, step: &BatchStep) -> Result<StmtResult> {
        let sql = step
            .stmt
            .sql
            .as_ref()
            .ok_or_else(|| anyhow!("No SQL in batch step"))?;

        debug!("Executing batch step SQL: {}", sql);

        let mut stmt = conn.prepare(sql)?;

        for (i, arg) in step.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value)?;
        }

        let want_rows = step.stmt.want_rows.unwrap_or(true);

        if want_rows {
            let rows = stmt.run_collect_rows()?;

            let cols: Vec<Col> = (0..stmt.num_columns())
                .map(|i| Col {
                    name: Some(stmt.get_column_name(i).to_string()),
                    decltype: stmt.get_column_decltype(i),
                })
                .collect();

            let result_rows: Vec<Row> = rows
                .into_iter()
                .map(|row| Row {
                    values: row.into_iter().map(convert_core_to_value).collect(),
                })
                .collect();

            Ok(StmtResult {
                cols,
                rows: result_rows,
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        } else {
            stmt.run_ignore_rows()?;
            Ok(StmtResult {
                cols: vec![],
                rows: vec![],
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        }
    }

    pub(crate) fn handle_pull_updates(
        &self,
        db: &Arc<DbHandle>,
        body: &[u8],
    ) -> Result<HttpResponse> {
        let req = <PullUpdatesReqProtoBody as Message>::decode(body)
            .map_err(|e| anyhow!("Failed to decode PullUpdatesRequest: {}", e))?;

        debug!(
            "Pull updates request: server_revision={}, client_revision={}",
            req.server_revision, req.client_revision
        );

        let encoding =
            PageUpdatesEncodingReq::try_from(req.encoding).unwrap_or(PageUpdatesEncodingReq::Raw);

        if encoding == PageUpdatesEncodingReq::Zstd {
            return Err(anyhow!("Zstd encoding is not supported"));
        }

        if PullUpdatesStreamKind::try_from(req.stream_kind).unwrap_or(PullUpdatesStreamKind::Pages)
            == PullUpdatesStreamKind::MvccLogicalLog
        {
            return self.handle_logical_pull_updates(db, &req);
        }

        {
            let mut state = db.state.lock().unwrap();
            if state.generations.is_some() {
                return self.handle_generation_pull_updates(db, &mut state, &req);
            }
        }
        self.handle_page_pull_updates(db, &req, PullUpdatesApplyMode::Incremental)
    }

    /// Page pull for WAL databases: revisions carry the generation, and pages
    /// are read at the revision's frame from the database file and the WAL.
    fn handle_generation_pull_updates(
        &self,
        db: &Arc<DbHandle>,
        state: &mut DbState,
        req: &PullUpdatesReqProtoBody,
    ) -> Result<HttpResponse> {
        let conn = state.conn.clone();
        let generations = state
            .generations
            .as_ref()
            .expect("caller checked generations");
        let (epoch, current) = (generations.epoch(), generations.current());
        let wal = conn.wal_state()?;
        let max_frame = wal.max_frame;

        let pinned = !req.server_revision.is_empty();
        let watermark = if pinned {
            match Revision::parse(&req.server_revision) {
                Revision::Generation {
                    epoch: e,
                    generation,
                    frame,
                } if e == epoch && generation == current && frame <= max_frame => frame,
                Revision::Legacy(frame) if current == 0 && frame <= max_frame => frame,
                _ => {
                    info!(
                        "pinned revision {} is no longer readable (current {})",
                        req.server_revision,
                        generations.revision(max_frame)
                    );
                    return Ok(text_response(
                        410,
                        "Revision is no longer available, restart the sync",
                    ));
                }
            }
        } else {
            max_frame
        };

        let selector: Option<RoaringBitmap> = if req.server_pages_selector.is_empty() {
            None
        } else {
            Some(
                RoaringBitmap::deserialize_from(&req.server_pages_selector[..])
                    .map_err(|e| anyhow!("Failed to parse server_pages_selector: {}", e))?,
            )
        };

        // 1-based page numbers changed since the client revision; `None` means
        // the client has to receive every page.
        let changed: Option<RoaringBitmap> = match Revision::parse(&req.client_revision) {
            Revision::Legacy(frame) if current == 0 && frame <= watermark => {
                Some(conn.wal_changed_pages_after(frame)?.into_iter().collect())
            }
            Revision::Generation {
                epoch: e,
                generation,
                frame,
            } if e == epoch => {
                if generation == current && frame <= watermark {
                    Some(conn.wal_changed_pages_after(frame)?.into_iter().collect())
                } else if generation < current {
                    match generations.changed_since(generation)? {
                        Some(mut sealed) => {
                            sealed.extend(conn.wal_changed_pages_after(0)?);
                            Some(sealed)
                        }
                        None => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        };
        if changed.is_none() && !req.client_revision.is_empty() {
            info!(
                "client revision {} predates the known history, sending the whole database",
                req.client_revision
            );
        }

        let db_size = current_snapshot_db_size_pages(&conn, watermark)?;
        let pages: Vec<u32> = match changed {
            Some(pages) => pages.into_iter().filter(|p| u64::from(*p) <= db_size).collect(),
            None => (1..=u32::try_from(db_size)?).collect(),
        };
        let pages: Vec<u32> = match &selector {
            Some(selector) => pages.into_iter().filter(|p| selector.contains(p - 1)).collect(),
            None => pages,
        };
        let header = PullUpdatesRespProtoBody {
            server_revision: Revision::format(epoch, current, watermark),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::Pages as i32,
        };
        let mut response_body = Vec::new();
        encode_length_delimited(&mut response_body, &header.encode_to_vec());
        if pinned || selector.is_some() {
            state.pinned_until = Some(Instant::now() + PINNED_READ_LEASE);
        }
        debug!(
            "pull-updates: client_revision={:?} server_revision={} pages={} db_size={db_size}",
            req.client_revision,
            header.server_revision,
            pages.len()
        );

        let stream = if pages.len() <= DOWNLOAD_BATCH_PAGES {
            encode_pages(&conn, &pages, watermark, &mut response_body)?;
            None
        } else {
            let download = PageDownload {
                db: db.clone(),
                pages,
                watermark,
                checkpoint_seq: wal.checkpoint_seq_no,
                db_file: db_file_meta(&db.path),
                stall_timeout: self.download_stall_timeout,
                _active: ActiveDownload::new(&state.active_downloads),
            };
            let (tx, rx) = mpsc::channel(DOWNLOAD_QUEUE_BATCHES);
            tokio::runtime::Handle::current().spawn(download.run(tx));
            Some(rx)
        };
        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
            stream,
        })
    }

    fn handle_page_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        apply_mode: PullUpdatesApplyMode,
    ) -> Result<HttpResponse> {
        let state = db.state.lock().unwrap();
        let conn = state.conn.clone();

        let wal_state = conn.wal_state()?;
        debug!("WAL state: max_frame={}", wal_state.max_frame);

        let server_revision: u64 = if req.server_revision.is_empty() {
            wal_state.max_frame
        } else {
            req.server_revision.parse().unwrap_or(wal_state.max_frame)
        };

        let client_revision: u64 = if req.client_revision.is_empty() {
            0
        } else {
            req.client_revision.parse().unwrap_or(0)
        };

        debug!(
            "Using server_revision={}, client_revision={}",
            server_revision, client_revision
        );

        let pages_selector: Option<RoaringBitmap> = if !req.server_pages_selector.is_empty() {
            Some(
                RoaringBitmap::deserialize_from(&req.server_pages_selector[..])
                    .map_err(|e| anyhow!("Failed to parse server_pages_selector: {}", e))?,
            )
        } else {
            None
        };

        let mut seen_pages: HashSet<u32> = HashSet::new();
        let mut pages_to_send: Vec<(u32, Vec<u8>)> = Vec::new();

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut frame_buffer = vec![0u8; frame_size];

        debug!(
            "pull-updates: scanning WAL frames {}..={} (client_revision={}, server_revision={})",
            client_revision + 1,
            server_revision,
            client_revision,
            server_revision
        );

        if server_revision > client_revision {
            for frame_no in (client_revision + 1..=server_revision).rev() {
                let frame_info = conn.wal_get_frame(frame_no, &mut frame_buffer)?;

                let page_no = frame_info.page_no;
                // WAL uses 1-based page numbers, sync protocol uses 0-based
                let page_id = page_no - 1;

                if seen_pages.contains(&page_no) {
                    continue;
                }

                if let Some(ref selector) = pages_selector {
                    if !selector.contains(page_id) {
                        continue;
                    }
                }

                seen_pages.insert(page_no);

                let type_byte = frame_buffer[WAL_FRAME_HEADER_SIZE];
                debug!(
                    "pull-updates: including page_no={}, frame_no={}, type_byte={}, db_size={}",
                    page_no, frame_no, type_byte, frame_info.db_size
                );

                let page_data = frame_buffer[WAL_FRAME_HEADER_SIZE..].to_vec();
                pages_to_send.push((page_id, page_data));
            }
        }

        debug!(
            "pull-updates: sending {} pages, seen_pages={:?}",
            pages_to_send.len(),
            seen_pages
        );
        pages_to_send.reverse();

        let db_size = current_db_size_pages(&conn, wal_state.max_frame)?;

        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: apply_mode as i32,
            mvcc_log: None,
            // The protocol hint reflects the database, not the response shape:
            // page bootstraps of an MVCC database advertise MvccLogical so
            // auto-detecting clients switch to logical pulls, mirroring the
            // production server.
            protocol: if conn.mvcc_enabled() {
                PullUpdatesProtocol::MvccLogical as i32
            } else {
                PullUpdatesProtocol::Pages as i32
            },
        };

        let mut response_body = Vec::new();

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page_data) in pages_to_send {
            let page_msg = PageData {
                page_id: page_id as u64,
                encoded_page: Bytes::from(page_data),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        debug!(
            "Sending {} bytes in pull-updates response",
            response_body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
            stream: None,
        })
    }

    fn handle_logical_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
    ) -> Result<HttpResponse> {
        let (db_size, fallback_revision, legacy_current_revision) = {
            let state = db.state.lock().unwrap();
            let conn = state.conn.clone();
            let wal_state = conn.wal_state()?;
            (
                current_db_size_pages(&conn, wal_state.max_frame)?,
                format!("page:{}", wal_state.max_frame),
                wal_state.max_frame.to_string(),
            )
        };
        let log_path = match logical_log_path(&db.path) {
            Ok(path) => path,
            Err(_) if is_in_memory_db_path(&db.path) => {
                info!(
                    "logical pull requested for in-memory sync server database; returning incremental pages"
                );
                return self.handle_page_pull_updates(db, req, PullUpdatesApplyMode::Incremental);
            }
            Err(err) => return Err(err),
        };
        let log = match std::fs::read(&log_path) {
            Ok(log) => log,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    "logical pull requested but no MVCC log exists at {}; returning replace-base fallback",
                    log_path.display()
                );
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err.into()),
        };
        let snapshot = match scan_mvcc_log(&log) {
            Ok(snapshot) => snapshot,
            Err(err) if is_nonportable_mvcc_log_error(&err) => {
                info!(
                    "logical pull requested but MVCC log is not portable; returning replace-base pages: {err}"
                );
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err),
        };
        let start_offset = parse_mvcc_revision_offset(&req.client_revision, snapshot.end_offset)?;
        if start_offset > snapshot.end_offset {
            return Err(anyhow!(
                "MVCC logical pull revision is from the future: client_offset={} server_offset={}",
                start_offset,
                snapshot.end_offset
            ));
        }
        let start = usize::try_from(start_offset)
            .map_err(|_| anyhow!("MVCC logical pull start offset overflows usize"))?;
        let end = usize::try_from(snapshot.end_offset)
            .map_err(|_| anyhow!("MVCC logical pull end offset overflows usize"))?;

        let mut response_body = Vec::new();
        let (mvcc_log, body) = if start == end {
            (None, Vec::new())
        } else {
            let crc_seed = if start_offset == 0 {
                None
            } else {
                let seed = snapshot.crc_seed_at(start_offset)?;
                Some(seed.to_le_bytes().to_vec())
            };
            (
                Some(MvccLogicalLogMetadataProto {
                    format: "lml3".to_string(),
                    checkpoint_transition: false,
                    ranges: vec![MvccLogicalLogRangeProto {
                        generation: 1,
                        start_offset,
                        end_offset: snapshot.end_offset,
                        starts_with_header: start_offset == 0,
                        crc_seed,
                    }],
                }),
                log[start..end].to_vec(),
            )
        };

        let header = PullUpdatesRespProtoBody {
            server_revision: format!("g1:o{}", snapshot.end_offset),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);
        response_body.extend_from_slice(&body);

        debug!(
            "pull-updates logical: path={} client_revision={} end_offset={} body_bytes={}",
            log_path.display(),
            req.client_revision,
            snapshot.end_offset,
            body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
            stream: None,
        })
    }

    fn handle_logical_fallback(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        server_revision: String,
        legacy_current_revision: &str,
        db_size: u64,
    ) -> Result<HttpResponse> {
        if req.client_revision == server_revision {
            return self.handle_empty_logical_pull(server_revision, db_size);
        }
        if req.client_revision == legacy_current_revision {
            return self.handle_empty_logical_pull(req.client_revision.clone(), db_size);
        }

        self.handle_replace_base_pages(db, server_revision)
    }

    fn handle_empty_logical_pull(
        &self,
        server_revision: String,
        db_size: u64,
    ) -> Result<HttpResponse> {
        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
            stream: None,
        })
    }

    fn handle_replace_base_pages(
        &self,
        db: &DbHandle,
        server_revision: String,
    ) -> Result<HttpResponse> {
        let (db_size, pages) = self.read_replace_base_pages(db)?;

        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
            mvcc_log: None,
            // Replace-base is only served from the MVCC logical flow here.
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page) in pages {
            let page_msg = PageData {
                page_id,
                encoded_page: Bytes::from(page),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
            stream: None,
        })
    }

    #[allow(clippy::type_complexity)]
    fn read_replace_base_pages(&self, db: &DbHandle) -> Result<(u64, Vec<(u64, Vec<u8>)>)> {
        let state = db.state.lock().unwrap();
        let conn = state.conn.clone();
        let wal_state = conn.wal_state()?;
        let frame_watermark = Some(wal_state.max_frame);
        let db_size = current_snapshot_db_size_pages(&conn, wal_state.max_frame)?;
        let pages_capacity = usize::try_from(db_size)
            .map_err(|_| anyhow!("database page count does not fit usize: {db_size}"))?;
        let mut pages = Vec::with_capacity(pages_capacity);

        for page_no in 1..=db_size {
            let page_no_u32 = u32::try_from(page_no)
                .map_err(|_| anyhow!("database page number does not fit u32: {page_no}"))?;
            let mut page = vec![0; PAGE_SIZE];
            let found =
                conn.try_wal_watermark_read_page(page_no_u32, &mut page, frame_watermark)?;
            if !found {
                return Err(anyhow!(
                    "database page {} is missing from replace-base snapshot",
                    page_no
                ));
            }
            pages.push((page_no - 1, page));
        }

        Ok((db_size, pages))
    }
}

pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) content_type: String,
    pub(crate) body: Vec<u8>,
    /// More of the body, sent after `body` as it is read.
    pub(crate) stream: Option<PageBatches>,
}

/// Batches of a streamed body. `None` marks the end; a channel closed
/// without it means the download failed, and the connection is aborted
/// instead of ending a truncated body as if it were complete.
pub(crate) type PageBatches = mpsc::Receiver<Option<Bytes>>;

/// Holds checkpoints off while a page download is in flight.
struct ActiveDownload(Arc<AtomicUsize>);

impl ActiveDownload {
    fn new(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for ActiveDownload {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Sends a large page set in batches and takes the database lock per batch,
/// so writes go on during the download. Pages are read at a fixed frame,
/// which stays readable as long as no checkpoint runs and the WAL is not
/// reset; a reset or a database file change aborts the download.
struct PageDownload {
    db: Arc<DbHandle>,
    pages: Vec<u32>,
    watermark: u64,
    checkpoint_seq: u32,
    db_file: (u64, u128),
    stall_timeout: Duration,
    _active: ActiveDownload,
}

impl PageDownload {
    async fn run(self, tx: mpsc::Sender<Option<Bytes>>) {
        let download = Arc::new(self);
        for start in (0..download.pages.len()).step_by(DOWNLOAD_BATCH_PAGES) {
            let reader = download.clone();
            let batch = match tokio::task::spawn_blocking(move || reader.read_batch(start)).await {
                Ok(Ok(batch)) => batch,
                Ok(Err(err)) => {
                    warn!("{}: page download aborted: {err:#}", download.db.path);
                    return;
                }
                Err(err) => {
                    error!("{}: page download failed: {err}", download.db.path);
                    return;
                }
            };
            match tokio::time::timeout(download.stall_timeout, tx.send(Some(batch))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return,
                Err(_) => {
                    info!("{}: dropping a client that stopped reading a page download", download.db.path);
                    return;
                }
            }
        }
        let _ = tx.send(None).await;
    }

    fn read_batch(&self, start: usize) -> Result<Bytes> {
        let end = (start + DOWNLOAD_BATCH_PAGES).min(self.pages.len());
        let state = self.db.state.lock().unwrap();
        if state.conn.wal_state()?.checkpoint_seq_no != self.checkpoint_seq {
            return Err(anyhow!("the WAL was reset during the download"));
        }
        if db_file_meta(&self.db.path) != self.db_file {
            return Err(anyhow!("the database file changed during the download"));
        }
        let mut out = Vec::with_capacity((end - start) * (PAGE_SIZE + 16));
        encode_pages(&state.conn, &self.pages[start..end], self.watermark, &mut out)?;
        Ok(Bytes::from(out))
    }
}

fn encode_pages(conn: &Connection, pages: &[u32], watermark: u64, out: &mut Vec<u8>) -> Result<()> {
    let mut page = vec![0u8; PAGE_SIZE];
    for &page_no in pages {
        if !conn.try_wal_watermark_read_page(page_no, &mut page, Some(watermark))? {
            return Err(anyhow!("page {page_no} is missing at frame {watermark}"));
        }
        let page_msg = PageData {
            page_id: u64::from(page_no - 1),
            encoded_page: Bytes::copy_from_slice(&page),
        };
        encode_length_delimited(out, &page_msg.encode_to_vec());
    }
    Ok(())
}

struct MvccLogSnapshot {
    end_offset: u64,
    crc_by_offset: Vec<(u64, u32)>,
}

impl MvccLogSnapshot {
    fn crc_seed_at(&self, offset: u64) -> Result<u32> {
        self.crc_by_offset
            .iter()
            .find_map(|(boundary, crc)| (*boundary == offset).then_some(*crc))
            .ok_or_else(|| {
                anyhow!("MVCC logical pull offset is not a transaction boundary: {offset}")
            })
    }
}

fn logical_log_path(db_path: &str) -> Result<PathBuf> {
    Ok(db_file_path(db_path)?.with_extension("db-log"))
}

fn is_in_memory_db_path(db_path: &str) -> bool {
    db_path == ":memory:"
}

fn db_file_path(db_path: &str) -> Result<PathBuf> {
    if is_in_memory_db_path(db_path) {
        return Err(anyhow!(
            "MVCC logical pull is not supported for in-memory sync server databases"
        ));
    }
    let path = if let Some(rest) = db_path.strip_prefix("file:") {
        rest.split_once('?').map_or(rest, |(path, _)| path)
    } else {
        db_path
    };
    Ok(PathBuf::from(path))
}

fn parse_mvcc_revision_offset(revision: &str, legacy_default: u64) -> Result<u64> {
    if revision.is_empty() {
        return Ok(0);
    }
    if let Some((generation, offset)) = revision.split_once(":o") {
        let generation = generation
            .strip_prefix('g')
            .ok_or_else(|| anyhow!("invalid MVCC pull revision generation: {revision}"))?
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision generation: {revision}: {err}"))?;
        if generation != 1 {
            return Err(anyhow!(
                "sync_server supports only single-generation MVCC logical pulls: {revision}"
            ));
        }
        return offset
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision offset: {revision}: {err}"));
    }
    // Older page bootstrap responses from this test server used WAL frame
    // numbers. Treat them as "the page snapshot already includes the current
    // logical log" so the required follow-up logical pull becomes a no-op.
    Ok(legacy_default)
}

fn scan_mvcc_log(log: &[u8]) -> Result<MvccLogSnapshot> {
    if log.is_empty() {
        return Ok(MvccLogSnapshot {
            end_offset: 0,
            crc_by_offset: vec![(0, 0)],
        });
    }
    if log.len() < MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "truncated MVCC logical log header: len={} header_size={}",
            log.len(),
            MVCC_LOG_HEADER_SIZE
        ));
    }
    validate_mvcc_log_header(log)?;
    let mut running_crc = initial_mvcc_log_crc(log)?;
    let mut offset = MVCC_LOG_HEADER_SIZE;
    let mut crc_by_offset = vec![(MVCC_LOG_HEADER_SIZE as u64, running_crc)];

    while offset < log.len() {
        let Some((frame_end, frame_crc)) = read_mvcc_frame_boundary(log, offset, running_crc)?
        else {
            break;
        };
        running_crc = frame_crc;
        offset = frame_end;
        crc_by_offset.push((offset as u64, running_crc));
    }

    Ok(MvccLogSnapshot {
        end_offset: offset as u64,
        crc_by_offset,
    })
}

fn is_nonportable_mvcc_log_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.starts_with("unsupported MVCC logical log version ")
}

fn validate_mvcc_log_header(log: &[u8]) -> Result<()> {
    if read_u32_le(log, 0)? != MVCC_LOG_MAGIC {
        return Err(anyhow!("invalid MVCC logical log magic"));
    }
    if log[4] != MVCC_LOG_VERSION {
        return Err(anyhow!("unsupported MVCC logical log version {}", log[4]));
    }
    if log[5] & 0b1111_1110 != 0 {
        return Err(anyhow!("invalid MVCC logical log header flags"));
    }
    let header_len = u16::from_le_bytes([log[6], log[7]]) as usize;
    if header_len != MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "invalid MVCC logical log header length: {header_len}"
        ));
    }
    if log[MVCC_LOG_HEADER_RESERVED_START..MVCC_LOG_HEADER_CRC_START]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(anyhow!(
            "MVCC logical log header reserved bytes must be zero"
        ));
    }
    let stored_crc = read_u32_le(log, MVCC_LOG_HEADER_CRC_START)?;
    let mut crc_buf = [0u8; MVCC_LOG_HEADER_SIZE];
    crc_buf.copy_from_slice(&log[..MVCC_LOG_HEADER_SIZE]);
    crc_buf[MVCC_LOG_HEADER_CRC_START..MVCC_LOG_HEADER_SIZE].fill(0);
    let expected_crc = crc32c::crc32c(&crc_buf);
    if stored_crc != expected_crc {
        return Err(anyhow!("MVCC logical log header checksum mismatch"));
    }
    Ok(())
}

fn initial_mvcc_log_crc(log: &[u8]) -> Result<u32> {
    let salt = u64::from_le_bytes(
        log[MVCC_LOG_HEADER_SALT_START..MVCC_LOG_HEADER_SALT_END]
            .try_into()
            .expect("fixed-size salt slice"),
    );
    Ok(crc32c::crc32c(&salt.to_le_bytes()))
}

fn read_mvcc_frame_boundary(
    log: &[u8],
    offset: usize,
    running_crc: u32,
) -> Result<Option<(usize, u32)>> {
    if log.len() - offset < MVCC_TX_HEADER_SIZE + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let frame_magic = read_u32_le(log, offset)?;
    let has_extension_header = frame_magic == MVCC_TX_EXT_FRAME_MAGIC;
    if frame_magic != MVCC_TX_FRAME_MAGIC && !has_extension_header {
        return Err(anyhow!(
            "invalid MVCC logical log frame magic at offset {offset}: {frame_magic:#x}"
        ));
    }
    let header_size = if has_extension_header {
        MVCC_TX_EXT_HEADER_SIZE
    } else {
        MVCC_TX_HEADER_SIZE
    };
    if log.len() - offset < header_size + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let payload_size = usize::try_from(read_u64_le(log, offset + 4)?)
        .map_err(|_| anyhow!("MVCC logical log payload size overflows usize"))?;
    let extension_size = if has_extension_header {
        let extension_size = usize::try_from(read_u64_le(log, offset + 24)?)
            .map_err(|_| anyhow!("MVCC logical log extension size overflows usize"))?;
        let extension_record_count = read_u32_le(log, offset + 32)?;
        let frame_flags = read_u32_le(log, offset + 36)?;
        if frame_flags & !MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK != 0 {
            return Err(anyhow!(
                "unsupported MVCC logical log frame flags at offset {offset}: {frame_flags:#x}"
            ));
        }
        if extension_size == 0 && extension_record_count != 0 {
            return Err(anyhow!(
                "MVCC logical log extension record count without extension block at offset {offset}"
            ));
        }
        if extension_size > 0 && frame_flags & MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK == 0 {
            return Err(anyhow!(
                "MVCC logical log extension block missing flag at offset {offset}"
            ));
        }
        extension_size
    } else {
        0
    };
    let trailer_start = offset
        .checked_add(header_size)
        .and_then(|value| value.checked_add(payload_size))
        .and_then(|value| value.checked_add(extension_size))
        .ok_or_else(|| anyhow!("MVCC logical log frame offset overflow"))?;
    let frame_end = trailer_start
        .checked_add(MVCC_TX_TRAILER_SIZE)
        .ok_or_else(|| anyhow!("MVCC logical log frame end overflow"))?;
    if frame_end > log.len() {
        return Ok(None);
    }
    let expected_crc = crc32c::crc32c_append(running_crc, &log[offset..trailer_start]);
    let stored_crc = read_u32_le(log, trailer_start)?;
    if stored_crc != expected_crc {
        return Err(anyhow!(
            "MVCC logical log frame checksum mismatch at offset {offset}"
        ));
    }
    let end_magic = read_u32_le(log, trailer_start + 4)?;
    if end_magic != MVCC_TX_END_MAGIC {
        return Err(anyhow!(
            "invalid MVCC logical log frame end magic at offset {offset}"
        ));
    }
    Ok(Some((frame_end, stored_crc)))
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let bytes = buf
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("buffer too short for u32 at offset {offset}"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let bytes = buf
        .get(offset..offset + 8)
        .ok_or_else(|| anyhow!("buffer too short for u64 at offset {offset}"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn current_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut last_frame = vec![0u8; frame_size];
        let last_info = conn.wal_get_frame(max_frame, &mut last_frame)?;
        Ok(last_info.db_size as u64)
    } else {
        Ok(0)
    }
}

fn current_snapshot_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        return current_db_size_pages(conn, max_frame);
    }

    let mut page = vec![0u8; PAGE_SIZE];
    if conn.try_wal_watermark_read_page(1, &mut page, Some(max_frame))? {
        Ok(db_size_from_page(&page) as u64)
    } else {
        Ok(0)
    }
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..32].try_into().unwrap())
}

fn text_response(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        content_type: "text/plain".to_string(),
        body: body.as_bytes().to_vec(),
        stream: None,
    }
}

fn validate_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn db_path_for(base: &Path, name: &str) -> PathBuf {
    // Extensionless like sqld's layout: the files appear as data, data-wal,
    // data-shm (and data.db-log under MVCC) inside the database's directory.
    base.join(name).join("data")
}

impl OpenHandles {
    fn new(capacity: usize) -> Self {
        Self {
            handles: HashMap::new(),
            capacity,
        }
    }

    fn get_or_open(
        &mut self,
        name: &str,
        open: impl FnOnce() -> std::result::Result<Arc<DbHandle>, HttpResponse>,
    ) -> std::result::Result<Arc<DbHandle>, HttpResponse> {
        if let Some(handle) = self.handles.get(name) {
            return Ok(handle.clone());
        }
        if self.handles.len() >= self.capacity {
            error!(
                "refusing to open {name}: {} databases are open",
                self.capacity
            );
            return Err(text_response(503, "Too many open databases"));
        }
        let handle = open()?;
        self.handles.insert(name.to_string(), handle.clone());
        Ok(handle)
    }
}

fn open_db_handle(
    path: &Path,
    config: &OpenConfig,
    backup: Option<Arc<Target>>,
) -> Result<Arc<DbHandle>> {
    let path_str = path.to_string_lossy().to_string();
    let (_io, db) = Database::open_new(
        &path_str,
        config.vfs.as_deref(),
        config.flags,
        config.db_opts,
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.wal_auto_actions_disable();
    let wal = conn.wal_state()?;

    let persistent = path_str != ":memory:" && !config.flags.contains(OpenFlags::ReadOnly);
    let generations = if conn.mvcc_enabled() {
        None
    } else {
        let mut generations = Generations::open(&path_str, persistent, config.keep_generations)?;
        if persistent {
            if let Some(identity) = generations.load_identity()? {
                if let Some(reason) = generations.detect_offline_change(&identity) {
                    warn!(
                        "{path_str}: {reason} while the server was down; \
                         clients will download the database again"
                    );
                    generations.seal(SealedPages::Unknown)?;
                }
            }
            generations.store_identity(wal.checkpoint_seq_no, wal.max_frame > 0)?;
        }
        info!(
            "{path_str}: generation {}, {} WAL frames",
            generations.current(),
            wal.max_frame
        );
        Some(generations)
    };

    let replica = match (backup, &generations, &config.backup) {
        (Some(target), Some(generations), Some(backup_config)) if persistent => Some(Replica::open(
            target,
            &path_str,
            generations.epoch(),
            generations.current(),
            backup_config.snapshot_interval,
        )?),
        (Some(_), None, _) => {
            warn!("{path_str}: MVCC databases are not backed up");
            None
        }
        _ => None,
    };
    let backup = replica
        .as_ref()
        .zip(config.backup.clone())
        .map(|(replica, config)| (replica.target().clone(), config));

    let handle = Arc::new(DbHandle {
        state: Mutex::new(DbState {
            conn,
            generations,
            checkpoint_seq: wal.checkpoint_seq_no,
            wal_salts_recorded: wal.max_frame > 0,
            pinned_until: None,
            last_checkpoint: Instant::now(),
            last_checkpoint_at: None,
            active_downloads: Arc::new(AtomicUsize::new(0)),
            replica,
        }),
        backup_idle: Condvar::new(),
        path: path_str,
    });
    if let Some((target, config)) = backup {
        info!("{}: backing up to {target}", handle.path);
        target
            .runtime()
            .spawn(back_up(handle.clone(), target.clone(), config));
    }
    Ok(handle)
}

/// Uploads new WAL frames and snapshots of one database in the background.
async fn back_up(handle: Arc<DbHandle>, target: Arc<Target>, config: BackupConfig) {
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut backoff = Duration::ZERO;
    loop {
        ticker.tick().await;
        tokio::time::sleep(backoff).await;
        match back_up_pending(&handle).await {
            Ok(snapshotted) => {
                if !backoff.is_zero() {
                    info!("{}: backup uploads to {target} work again", handle.path);
                    backoff = Duration::ZERO;
                }
                if let (true, Some(retention)) = (snapshotted, config.retention) {
                    match replica::prune(&target, retention).await {
                        Ok(0) => {}
                        Ok(removed) => {
                            info!("{}: removed {removed} expired backup generations", handle.path)
                        }
                        Err(err) => warn!("{}: failed to prune the backup: {err:#}", handle.path),
                    }
                }
            }
            Err(err) => {
                if backoff.is_zero() {
                    error!("{}: backup upload to {target} failed, retrying: {err:#}", handle.path);
                }
                backoff = (backoff * 2).clamp(Duration::from_secs(1), BACKUP_MAX_BACKOFF);
            }
        }
    }
}

/// Runs backup jobs until nothing is left. Returns whether a new snapshot
/// became restorable.
async fn back_up_pending(handle: &Arc<DbHandle>) -> Result<bool> {
    let mut snapshotted = false;
    loop {
        let db = handle.clone();
        let job = tokio::task::spawn_blocking(move || -> Result<Option<Job>> {
            let job = db.state.lock().unwrap().backup_job()?;
            let Some(mut job) = job else {
                return Ok(None);
            };
            match job.prepare() {
                Ok(()) => Ok(Some(job)),
                Err(err) => {
                    db.finish_backup_job(&job, false)?;
                    Err(err)
                }
            }
        })
        .await??;
        let Some(job) = job else {
            return Ok(snapshotted);
        };
        let result = job.run().await;
        let succeeded = result.is_ok();
        let db = handle.clone();
        snapshotted |=
            tokio::task::spawn_blocking(move || db.finish_backup_job(&job, succeeded)).await??;
        result?;
    }
}

fn encode_length_delimited(output: &mut Vec<u8>, data: &[u8]) {
    let mut len = data.len();
    while len >= 0x80 {
        output.push((len as u8) | 0x80);
        len >>= 7;
    }
    output.push(len as u8);
    output.extend_from_slice(data);
}

fn convert_value_to_core(value: &Value) -> CoreValue {
    match value {
        Value::None | Value::Null => CoreValue::Null,
        Value::Integer { value } => CoreValue::from_i64(*value),
        Value::Float { value } => CoreValue::from_f64(*value),
        Value::Text { value } => CoreValue::Text(turso_core::types::Text {
            value: std::borrow::Cow::Owned(value.clone()),
            subtype: turso_core::types::TextSubtype::Text,
        }),
        Value::Blob { value } => CoreValue::Blob(value.to_vec()),
    }
}

fn convert_core_to_value(value: CoreValue) -> Value {
    match value {
        CoreValue::Null => Value::Null,
        CoreValue::Numeric(turso_core::Numeric::Integer(v)) => Value::Integer { value: v },
        CoreValue::Numeric(turso_core::Numeric::Float(v)) => Value::Float {
            value: f64::from(v),
        },
        CoreValue::Text(t) => Value::Text {
            value: t.value.to_string(),
        },
        CoreValue::Blob(b) => Value::Blob {
            value: Bytes::from(b),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn validates_database_names() {
        for ok in ["db1", "a-b_c", "x", "nul", "con", &"n".repeat(128)] {
            assert!(validate_db_name(ok), "expected {ok:?} to be valid");
        }
        for bad in [
            "",
            "..",
            "../x",
            "a/b",
            "a\\b",
            ".hidden",
            "a.b",
            "a%2fb",
            "a b",
            "A1",
            "Db1",
            "DB1",
            &"n".repeat(129),
        ] {
            assert!(!validate_db_name(bad), "expected {bad:?} to be rejected");
        }
    }

    #[test]
    fn each_database_resolves_its_own_files() {
        let base = Path::new("/tmp/dbs");
        let db1 = db_path_for(base, "db1");
        let db2 = db_path_for(base, "db2");

        assert_eq!(db1, Path::new("/tmp/dbs/db1/data"));
        assert_ne!(db1, db2);

        let log1 = logical_log_path(&db1.to_string_lossy()).unwrap();
        let log2 = logical_log_path(&db2.to_string_lossy()).unwrap();
        assert_ne!(log1, log2, "databases must not share a logical log");
        assert_eq!(log1, Path::new("/tmp/dbs/db1/data.db-log"));
    }

    const TEST_MAX_OPEN: usize = 4;

    fn dir_server(base: &Path) -> TursoSyncServer {
        TursoSyncServer::new_dir(
            base.to_path_buf(),
            OpenConfig {
                vfs: None,
                flags: OpenFlags::default(),
                db_opts: DatabaseOpts::new(),
                max_open: TEST_MAX_OPEN,
                keep_generations: 16,
                backup: None,
            },
            CheckpointConfig {
                interval: None,
                min_frames: 1,
                after_write: false,
            },
        )
        .unwrap()
    }

    #[test]
    fn refuses_new_databases_once_the_open_map_is_full() {
        let base = tempfile::TempDir::new().unwrap();
        let server = dir_server(base.path());

        for i in 0..TEST_MAX_OPEN {
            let name = format!("db{i}");
            if let Err(resp) = server.resolve_db(Some(&name)) {
                panic!(
                    "opening {name} under the cap must succeed, got {}",
                    resp.status
                );
            }
        }

        assert!(
            server.resolve_db(Some("db0")).is_ok(),
            "an already open database stays reachable at capacity"
        );
        let Err(refused) = server.resolve_db(Some("overflow")) else {
            panic!("a full open map must refuse an unknown database");
        };
        assert_eq!(refused.status, 503);
        assert!(
            !base.path().join("overflow").exists(),
            "a refused database must not reach the filesystem"
        );
    }

    #[test]
    fn refuses_a_database_directory_that_escapes_the_served_tree() {
        let base = tempfile::TempDir::new().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), base.path().join("escaped")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(outside.path(), base.path().join("escaped")).unwrap();

        let server = dir_server(base.path());
        let Err(refused) = server.resolve_db(Some("escaped")) else {
            panic!("a symlinked database directory must be refused");
        };
        assert_eq!(refused.status, 404);
        assert!(
            !outside.path().join("data").exists(),
            "a refused name must not create files outside the served tree"
        );
    }
}
