//! Continuous backup of WAL databases to S3 or a directory, in the spirit of
//! sqld's bottomless.
//!
//! Each local generation (see [`crate::generations`]) is backed up as a
//! generation of its own under `{root}/generations/{id}/`:
//!
//! - `snapshot.zst`: the database file the generation's WAL applies to;
//! - `meta`: `snapshot`, or `parent {id}` when the generation continues the
//!   final state of its parent instead. A generation without `meta` is not
//!   restorable yet, and restore skips it;
//! - `frames/{first}-{last}.zst`: WAL frames, numbered from 1 within the
//!   generation.
//!
//! Ids start with the creation time, so they list chronologically. A restore
//! takes the newest restorable generation, follows `parent` links back to a
//! snapshot and replays the frames of every generation on the way.
//!
//! A generation continues its parent only if the parent's WAL was uploaded in
//! full before the checkpoint that ended it. Every other start (first run, a
//! WAL reset behind the server's back, lost local state, a point-in-time
//! restore) takes a snapshot, and so does the first generation after the
//! snapshot interval.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use futures_util::stream::FuturesOrdered;
use futures_util::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, PutPayloadMut, WriteMultipart};
use tokio::runtime::Handle;
use tracing::{info, warn};
use turso_core::types::WalState;
use turso_core::Connection;

use crate::crash;
use crate::generations::write_atomic;
use crate::metrics::{metrics, UploadLabels};

const STATE_FILE: &str = "backup";
const RESTORED_FILE: &str = "backup-restored";
const SEGMENT_MAGIC: &[u8; 8] = b"TSWALSG1";
const SEGMENT_HEADER_SIZE: usize = 24;
const WAL_FRAME_HEADER_SIZE: usize = 24;
const MAX_SEGMENT_FRAMES: u64 = 4096;
const ZSTD_LEVEL: i32 = 3;
const SNAPSHOT_PART_SIZE: usize = 8 * 1024 * 1024;
const PARALLEL_DOWNLOADS: usize = 32;
/// Compressed bytes a restore downloads ahead of the frames it replays.
const RESTORE_BUFFER_BYTES: u64 = 32 * 1024 * 1024;
const ID_TIME_FORMAT: &str = "%Y%m%dT%H%M%S%3f";
/// Uploads made for a checkpoint run under the database lock.
const INLINE_UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Where backups go: an S3 bucket, configured through the standard `AWS_*`
/// variables, or a local directory.
pub struct Target {
    store: Arc<dyn ObjectStore>,
    root: Path,
    url: String,
    runtime: Handle,
    /// Set for directory backups, which keep empty directories around.
    local_dir: Option<PathBuf>,
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url)
    }
}

impl Target {
    /// Must be called inside a tokio runtime, which later uploads run on.
    pub fn open(url: &str) -> Result<Arc<Self>> {
        let runtime = Handle::try_current().context("backups need a tokio runtime")?;
        let url = url.trim_end_matches('/');
        let mut local_dir = None;
        let (store, root): (Arc<dyn ObjectStore>, &str) =
            if let Some(rest) = url.strip_prefix("s3://") {
                let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
                if bucket.is_empty() {
                    bail!("the backup URL {url} names no bucket");
                }
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .with_context(|| format!("failed to configure S3 for {url}"))?;
                (Arc::new(store), prefix)
            } else {
                let dir = url.strip_prefix("file://").unwrap_or(url);
                if !dir.starts_with('/') {
                    bail!("the backup URL must be s3://bucket/prefix or an absolute directory: {url}");
                }
                fs::create_dir_all(dir).with_context(|| format!("failed to create {dir}"))?;
                local_dir = Some(PathBuf::from(dir));
                (Arc::new(LocalFileSystem::new_with_prefix(dir)?), "")
            };
        Ok(Arc::new(Self {
            store,
            root: Path::parse(root)?,
            url: url.to_string(),
            runtime,
            local_dir,
        }))
    }

    /// The backup of one database under `--sync-dir`.
    pub fn child(&self, name: &str) -> Arc<Self> {
        Arc::new(Self {
            store: self.store.clone(),
            root: self.root.clone().join(name),
            url: format!("{}/{name}", self.url),
            runtime: self.runtime.clone(),
            local_dir: self.local_dir.clone(),
        })
    }

    /// An in-memory backup whose deletes each take `delete_wait`.
    #[cfg(test)]
    pub(crate) fn slow_deletes(delete_wait: Duration) -> Arc<Self> {
        use object_store::memory::InMemory;
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let config = ThrottleConfig { wait_delete_per_call: delete_wait, ..Default::default() };
        Arc::new(Self {
            store: Arc::new(ThrottledStore::new(InMemory::new(), config)),
            root: Path::default(),
            url: "memory://".to_string(),
            runtime: Handle::current(),
            local_dir: None,
        })
    }

    /// Adds an empty generation from `days` ago and returns its id.
    #[cfg(test)]
    pub(crate) async fn add_old_generation(&self, days: i64, meta: Meta) -> String {
        let time = Utc::now() - TimeDelta::days(days);
        let id = format!("{}Z-00000001-{days}", time.format(ID_TIME_FORMAT));
        self.store.put(&self.generation(&id).join("meta"), meta.encode().into()).await.unwrap();
        id
    }

    #[cfg(test)]
    pub(crate) async fn has_generation(&self, id: &str) -> bool {
        self.list_ids().await.unwrap().iter().any(|known| known == id)
    }

    pub fn runtime(&self) -> &Handle {
        &self.runtime
    }

    fn generations(&self) -> Path {
        self.root.clone().join("generations")
    }

    fn generation(&self, id: &str) -> Path {
        self.generations().join(id)
    }

    async fn list_ids(&self) -> Result<Vec<String>> {
        let listing = match self.store.list_with_delimiter(Some(&self.generations())).await {
            Ok(listing) => listing,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut ids: Vec<String> = listing
            .common_prefixes
            .iter()
            .filter_map(|prefix| prefix.filename().map(str::to_string))
            .collect();
        ids.sort();
        Ok(ids)
    }

    async fn meta(&self, id: &str) -> Result<Option<Meta>> {
        match self.store.get(&self.generation(id).join("meta")).await {
            Ok(result) => Ok(Some(Meta::decode(std::str::from_utf8(&result.bytes().await?)?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Frame objects of a generation uploaded up to `at`, ordered for replay.
    async fn segments(&self, id: &str, at: Option<DateTime<Utc>>) -> Result<Vec<SegmentObject>> {
        let prefix = self.generation(id).join("frames");
        let listing = match self.store.list_with_delimiter(Some(&prefix)).await {
            Ok(listing) => listing,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut segments: Vec<SegmentObject> = listing
            .objects
            .into_iter()
            .filter(|object| at.is_none_or(|at| object.last_modified <= at))
            .filter_map(|object| {
                let (first, last) = parse_segment_name(object.location.filename()?)?;
                Some(SegmentObject {
                    first,
                    last,
                    location: object.location,
                    uploaded: object.last_modified,
                    size: object.size,
                })
            })
            .collect();
        segments.sort_by_key(|segment| (segment.first, Reverse(segment.last)));
        Ok(segments)
    }

    async fn remove_generation(&self, id: &str) -> Result<()> {
        let locations = self.store.list(Some(&self.generation(id))).map_ok(|object| object.location);
        // S3 deletes up to 1000 objects per request this way.
        let mut deleted = self.store.delete_stream(locations.boxed());
        while let Some(result) = deleted.next().await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        if let Some(dir) = &self.local_dir {
            match fs::remove_dir_all(dir.join(self.generation(id).as_ref())) {
                Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err.into()),
                _ => {}
            }
        }
        Ok(())
    }
}

struct SegmentObject {
    first: u64,
    last: u64,
    location: Path,
    uploaded: DateTime<Utc>,
    size: u64,
}

fn segment_name(first: u64, last: u64) -> String {
    format!("{first:012}-{last:012}.zst")
}

fn parse_segment_name(name: &str) -> Option<(u64, u64)> {
    let (first, last) = name.strip_suffix(".zst")?.split_once('-')?;
    let (first, last) = (first.parse().ok()?, last.parse().ok()?);
    (first >= 1 && first <= last).then_some((first, last))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Meta {
    Snapshot,
    Parent(String),
}

impl Meta {
    fn encode(&self) -> String {
        match self {
            Self::Snapshot => "snapshot\n".to_string(),
            Self::Parent(id) => format!("parent {id}\n"),
        }
    }

    fn decode(text: &str) -> Result<Self> {
        let text = text.trim();
        if text == "snapshot" {
            return Ok(Self::Snapshot);
        }
        match text.strip_prefix("parent ") {
            Some(id) if !id.is_empty() => Ok(Self::Parent(id.to_string())),
            _ => Err(anyhow!("malformed backup meta: {text:?}")),
        }
    }
}

impl std::fmt::Display for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snapshot => f.write_str("snapshot"),
            Self::Parent(id) => write!(f, "continues {id}"),
        }
    }
}

/// Ids sort by creation time, and later than `after` even if the clock went back.
fn new_id(epoch: u32, generation: u64, after: Option<&str>) -> String {
    let mut now = Utc::now();
    if let Some(previous) = after.and_then(id_time) {
        if now <= previous {
            now = previous + TimeDelta::milliseconds(1);
        }
    }
    format!("{}Z-{epoch:08x}-{generation}", now.format(ID_TIME_FORMAT))
}

fn id_time(id: &str) -> Option<DateTime<Utc>> {
    let stamp = id.get(..18)?;
    NaiveDateTime::parse_from_str(stamp, ID_TIME_FORMAT)
        .ok()
        .map(|time| time.and_utc())
}

/// Backup progress of one database, kept in `{db}-sync/backup`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct State {
    epoch: u32,
    generation: u64,
    /// The backup generation receiving the WAL of the local generation.
    id: Option<String>,
    /// What `id` builds on; `None` until its snapshot is uploaded.
    meta: Option<Meta>,
    /// Whether `meta` is uploaded, which makes `id` restorable.
    established: bool,
    /// WAL frames uploaded under `id`.
    shipped: u64,
    /// The generation the next one continues; `None` starts it from a snapshot.
    parent: Option<String>,
    /// The newest id created here, which later ids must sort after.
    last_id: Option<String>,
    /// Unix time of the snapshot the chain last started from.
    snapshot_at: i64,
}

impl State {
    fn encode(&self) -> String {
        let opt = |value: &Option<String>| value.clone().unwrap_or_else(|| "-".to_string());
        format!(
            "epoch {:08x}\ngeneration {}\nid {}\nmeta {}\nestablished {}\nshipped {}\nparent {}\nlast_id {}\nsnapshot_at {}\n",
            self.epoch,
            self.generation,
            opt(&self.id),
            opt(&self.meta.as_ref().map(|meta| meta.encode().trim().to_string())),
            u8::from(self.established),
            self.shipped,
            opt(&self.parent),
            opt(&self.last_id),
            self.snapshot_at,
        )
    }

    fn decode(text: &str) -> Result<Self> {
        let fields: BTreeMap<&str, &str> = text
            .lines()
            .filter_map(|line| line.split_once(' '))
            .collect();
        let get = |key: &str| {
            fields
                .get(key)
                .copied()
                .ok_or_else(|| anyhow!("backup state lacks {key}"))
        };
        let opt = |key: &str| -> Result<Option<String>> {
            Ok(Some(get(key)?).filter(|v| *v != "-").map(str::to_string))
        };
        Ok(Self {
            epoch: u32::from_str_radix(get("epoch")?, 16)?,
            generation: get("generation")?.parse()?,
            id: opt("id")?,
            meta: opt("meta")?.map(|meta| Meta::decode(&meta)).transpose()?,
            established: get("established")? == "1",
            shipped: get("shipped")?.parse()?,
            parent: opt("parent")?,
            last_id: opt("last_id")?,
            snapshot_at: get("snapshot_at")?.parse()?,
        })
    }
}

/// Backs up one database. Lives in the database state, so every method runs
/// under the database lock; uploads that take long run without it, between
/// [`Replica::next_job`] and [`Replica::finish`].
pub struct Replica {
    target: Arc<Target>,
    db_file: PathBuf,
    state_path: PathBuf,
    state: State,
    busy: bool,
    snapshot_interval: Duration,
    /// When the backup last held every committed frame.
    synced_at: Option<SystemTime>,
    /// Generations that ended since [`Replica::take_finished`] last ran.
    finished: Vec<String>,
}

impl Replica {
    pub fn open(
        target: Arc<Target>,
        db_path: &str,
        epoch: u32,
        generation: u64,
        snapshot_interval: Duration,
    ) -> Result<Self> {
        let sync_dir = PathBuf::from(format!("{db_path}-sync"));
        let state_path = sync_dir.join(STATE_FILE);
        let restored_path = sync_dir.join(RESTORED_FILE);
        let loaded = match fs::read_to_string(&state_path) {
            Ok(text) => match State::decode(&text) {
                Ok(state) => Some(state),
                Err(err) => {
                    warn!(
                        "{db_path}: unreadable {} ({err}); the backup starts over from a snapshot",
                        state_path.display()
                    );
                    None
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        let state = match loaded {
            Some(state) => state,
            None => {
                let mut state = State {
                    epoch,
                    generation,
                    ..State::default()
                };
                match fs::read_to_string(&restored_path) {
                    Ok(text) => {
                        let (tip, root) = text
                            .trim()
                            .split_once(' ')
                            .ok_or_else(|| anyhow!("malformed {}", restored_path.display()))?;
                        state.parent = Some(tip.to_string());
                        state.last_id = Some(tip.to_string());
                        state.snapshot_at = id_time(root).map_or(0, |time| time.timestamp());
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
                state
            }
        };
        let mut replica = Self {
            target,
            db_file: PathBuf::from(db_path),
            state_path,
            state,
            busy: false,
            snapshot_interval,
            synced_at: None,
            finished: Vec::new(),
        };
        replica.align(epoch, generation, None);
        replica.persist()?;
        match fs::remove_file(&restored_path) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err.into()),
            _ => {}
        }
        Ok(replica)
    }

    pub fn target(&self) -> &Arc<Target> {
        &self.target
    }

    /// An upload runs without the database lock; checkpoints have to wait.
    pub fn busy(&self) -> bool {
        self.busy
    }

    fn persist(&self) -> Result<()> {
        write_atomic(&self.state_path, self.state.encode().as_bytes())
    }

    /// Records upload progress without syncing it to disk. Runs every backup
    /// tick under the database lock, where an fsync would stall requests, and
    /// a progress record lost in a power failure only repeats some uploads.
    fn persist_progress(&self) -> Result<()> {
        let tmp = self.state_path.with_extension("tmp");
        fs::write(&tmp, self.state.encode())?;
        fs::rename(&tmp, &self.state_path)?;
        Ok(())
    }

    /// Committed frames of the current WAL that the backup lacks.
    pub fn pending_frames(&self, max_frame: u64) -> u64 {
        let shipped = if self.state.established { self.state.shipped } else { 0 };
        max_frame.saturating_sub(shipped)
    }

    /// Whether the backup cannot restore the current generation until a
    /// snapshot is uploaded.
    pub fn snapshot_pending(&self) -> bool {
        match self.state.id {
            Some(_) => self.state.meta.is_none(),
            None => self.state.parent.is_none(),
        }
    }

    /// Generations that will get no more uploads, for [`compact`].
    pub fn take_finished(&mut self) -> Vec<String> {
        std::mem::take(&mut self.finished)
    }

    pub fn synced_at(&self) -> Option<SystemTime> {
        self.synced_at
    }

    /// When the chain last started from a snapshot.
    pub fn snapshot_at(&self) -> Option<SystemTime> {
        u64::try_from(self.state.snapshot_at)
            .ok()
            .filter(|secs| *secs > 0)
            .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
    }

    /// No upload to do right now; notes whether that means the backup is complete.
    fn idle(&mut self, max_frame: u64) -> Option<Job> {
        if self.pending_frames(max_frame) == 0 && !self.snapshot_pending() {
            self.synced_at = Some(SystemTime::now());
        }
        None
    }

    fn snapshot_due(&self) -> bool {
        let age = Utc::now().timestamp() - self.state.snapshot_at;
        age < 0 || age as u64 >= self.snapshot_interval.as_secs()
    }

    /// Starts over when the local generation moved on without a checkpoint
    /// passing through here: a WAL reset by a request, changes made while the
    /// server was down, or a crash in the middle of a checkpoint.
    fn align(&mut self, epoch: u32, generation: u64, max_frame: Option<u64>) -> bool {
        let moved = (self.state.epoch, self.state.generation) != (epoch, generation);
        let shrunk = max_frame.is_some_and(|max| max < self.state.shipped);
        if moved || shrunk {
            self.state = State {
                epoch,
                generation,
                last_id: self.state.last_id.take(),
                snapshot_at: self.state.snapshot_at,
                ..State::default()
            };
        }
        moved || shrunk
    }

    /// The next upload, if any. A snapshot reads the database file, which only
    /// stays put while no checkpoint runs, so callers that are about to
    /// checkpoint pass `allow_snapshot = false`.
    pub fn next_job(
        &mut self,
        conn: &Connection,
        epoch: u32,
        generation: u64,
        wal: &WalState,
        allow_snapshot: bool,
    ) -> Result<Option<Job>> {
        if self.busy {
            return Ok(None);
        }
        if self.align(epoch, generation, Some(wal.max_frame)) {
            self.persist()?;
        }
        if self.state.id.is_none() {
            let continues = self.state.parent.clone().filter(|_| !self.snapshot_due());
            if continues.is_some() && wal.max_frame == 0 {
                return Ok(self.idle(wal.max_frame));
            }
            if continues.is_none() && !allow_snapshot {
                return Ok(self.idle(wal.max_frame));
            }
            let id = new_id(epoch, generation, self.state.last_id.as_deref());
            self.state.id = Some(id.clone());
            self.state.last_id = Some(id);
            self.state.meta = continues.map(Meta::Parent);
            self.persist()?;
        }
        let id = self.state.id.clone().expect("assigned above");
        let Some(meta) = self.state.meta.clone() else {
            if !allow_snapshot {
                return Ok(self.idle(wal.max_frame));
            }
            self.busy = true;
            return Ok(Some(Job::Snapshot {
                target: self.target.clone(),
                id,
                seq: wal.checkpoint_seq_no,
                db_file: self.db_file.clone(),
            }));
        };
        let meta = (!self.state.established).then_some(meta);
        let segment = if wal.max_frame > self.state.shipped {
            let first = self.state.shipped + 1;
            let last = wal.max_frame.min(self.state.shipped + MAX_SEGMENT_FRAMES);
            Some(Segment::read(conn, first, last)?)
        } else {
            None
        };
        if meta.is_none() && segment.is_none() {
            return Ok(self.idle(wal.max_frame));
        }
        self.busy = true;
        Ok(Some(Job::Upload {
            target: self.target.clone(),
            id,
            meta,
            segment,
        }))
    }

    pub fn abandon(&mut self) {
        self.busy = false;
    }

    /// Records the outcome of a job from [`Replica::next_job`]. Returns whether
    /// a new snapshot became restorable.
    pub fn finish(&mut self, job: &Job, succeeded: bool, wal: &WalState) -> Result<bool> {
        if succeeded {
            crash::point(&format!("backup.{}-uploaded", job.kind()));
        }
        self.busy = false;
        let labels = UploadLabels {
            db: self.db_file.display().to_string(),
            kind: job.kind(),
            result: if succeeded { "ok" } else { "error" },
        };
        metrics().backup_uploads.get_or_create(&labels).inc();
        if let (true, Job::Upload { segment: Some(segment), .. }) = (succeeded, job) {
            let bytes = segment.compressed.as_ref().map_or(0, Vec::len);
            metrics().backup_uploaded_bytes.get_or_create(&labels).inc_by(bytes as u64);
        }
        if !succeeded || self.state.id.as_deref() != Some(job.id()) {
            return Ok(false);
        }
        let mut snapshotted = false;
        match job {
            Job::Snapshot { seq, .. } => {
                if wal.checkpoint_seq_no != *seq {
                    warn!(
                        "{}: the WAL was reset during the snapshot upload, taking it again",
                        self.db_file.display()
                    );
                    return Ok(false);
                }
                self.state.meta = Some(Meta::Snapshot);
                self.state.snapshot_at = Utc::now().timestamp();
            }
            Job::Upload { meta, segment, .. } => {
                if let Some(segment) = segment {
                    self.state.shipped = self.state.shipped.max(segment.last);
                }
                if meta.is_none() {
                    self.persist_progress()?;
                    return Ok(false);
                }
                self.state.established = true;
                snapshotted = self.state.meta == Some(Meta::Snapshot);
            }
        }
        self.persist()?;
        Ok(snapshotted)
    }

    /// Uploads the rest of the WAL while the caller holds the database lock,
    /// before a checkpoint or at shutdown. A snapshot (only allowed at
    /// shutdown) is not bounded by the upload timeout. Returns whether the
    /// backup holds the whole WAL, which lets the next generation continue
    /// this one.
    pub fn flush_blocking(
        &mut self,
        conn: &Connection,
        epoch: u32,
        generation: u64,
        allow_snapshot: bool,
    ) -> Result<bool> {
        loop {
            let wal = conn.wal_state()?;
            let Some(mut job) = self.next_job(conn, epoch, generation, &wal, allow_snapshot)?
            else {
                return Ok(self.state.established && self.state.shipped >= wal.max_frame);
            };
            let result = job.prepare().and_then(|()| {
                self.target.runtime.block_on(async {
                    if matches!(job, Job::Snapshot { .. }) {
                        return job.run().await;
                    }
                    tokio::time::timeout(INLINE_UPLOAD_TIMEOUT, job.run())
                        .await
                        .map_err(|_| anyhow!("upload timed out"))?
                })
            });
            self.finish(&job, result.is_ok(), &wal)?;
            result?;
        }
    }

    /// Moves on to the next local generation after a checkpoint.
    pub fn on_checkpoint(&mut self, epoch: u32, generation: u64, backed_up: bool) -> Result<()> {
        self.finished.extend(self.state.id.clone());
        let parent = if backed_up { self.state.id.take() } else { None };
        self.state = State {
            epoch,
            generation,
            parent,
            last_id: self.state.last_id.take(),
            snapshot_at: self.state.snapshot_at,
            ..State::default()
        };
        self.persist()
    }
}

/// WAL frames `first..=last` in their on-disk form, behind a small header.
pub struct Segment {
    first: u64,
    last: u64,
    bytes: Vec<u8>,
    compressed: Option<Vec<u8>>,
}

impl Segment {
    fn read(conn: &Connection, first: u64, last: u64) -> Result<Self> {
        let page_size = conn.get_page_size().get();
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size as usize;
        let count = last - first + 1;
        let mut bytes = Vec::with_capacity(SEGMENT_HEADER_SIZE + count as usize * frame_size);
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes.extend_from_slice(&page_size.to_be_bytes());
        bytes.extend_from_slice(&first.to_be_bytes());
        bytes.extend_from_slice(&u32::try_from(count)?.to_be_bytes());
        let mut frame = vec![0u8; frame_size];
        for frame_no in first..=last {
            conn.wal_get_frame(frame_no, &mut frame)?;
            bytes.extend_from_slice(&frame);
        }
        Ok(Self {
            first,
            last,
            bytes,
            compressed: None,
        })
    }
}

pub enum Job {
    Snapshot {
        target: Arc<Target>,
        id: String,
        /// The WAL must still be the same one when the upload ends.
        seq: u32,
        db_file: PathBuf,
    },
    Upload {
        target: Arc<Target>,
        id: String,
        /// Uploaded first: it makes the generation restorable.
        meta: Option<Meta>,
        segment: Option<Segment>,
    },
}

impl Job {
    fn id(&self) -> &str {
        match self {
            Self::Snapshot { id, .. } | Self::Upload { id, .. } => id,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::Upload { .. } => "frames",
        }
    }

    /// CPU work that should not hold the database lock.
    pub fn prepare(&mut self) -> Result<()> {
        if let Self::Upload {
            segment: Some(segment),
            ..
        } = self
        {
            if segment.compressed.is_none() {
                segment.compressed = Some(compress(&segment.bytes)?);
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        match self {
            Self::Snapshot {
                target,
                id,
                db_file,
                ..
            } => {
                let (target, id, db_file) = (target.clone(), id.clone(), db_file.clone());
                tokio::task::spawn_blocking(move || upload_snapshot(&target, &id, &db_file)).await?
            }
            Self::Upload {
                target,
                id,
                meta,
                segment,
            } => {
                let generation = target.generation(id);
                if let Some(meta) = meta {
                    target
                        .store
                        .put(&generation.clone().join("meta"), meta.encode().into())
                        .await?;
                }
                if let Some(segment) = segment {
                    let body = match &segment.compressed {
                        Some(compressed) => compressed.clone(),
                        None => compress(&segment.bytes)?,
                    };
                    let location = generation
                        .join("frames")
                        .join(segment_name(segment.first, segment.last).as_str());
                    target.store.put(&location, body.into()).await?;
                }
                Ok(())
            }
        }
    }
}

fn compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), ZSTD_LEVEL)?;
    encoder.include_checksum(true)?;
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

/// Streams the compressed database file into a multipart upload. Runs on a
/// blocking thread.
fn upload_snapshot(target: &Target, id: &str, db_file: &FsPath) -> Result<()> {
    let runtime = &target.runtime;
    let location = target.generation(id).join("snapshot.zst");
    let upload = runtime.block_on(target.store.put_multipart(&location))?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, SNAPSHOT_PART_SIZE);
    let mut uploaded = 0;
    let mut write = || -> Result<()> {
        let mut encoder = zstd::Encoder::new(Vec::new(), ZSTD_LEVEL)?;
        encoder.include_checksum(true)?;
        let mut file = match File::open(db_file) {
            Ok(file) => Some(file),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        let mut buf = vec![0u8; 1024 * 1024];
        while let Some(file) = file.as_mut() {
            let read = file.read(&mut buf)?;
            if read == 0 {
                break;
            }
            encoder.write_all(&buf[..read])?;
            if encoder.get_ref().len() >= SNAPSHOT_PART_SIZE {
                runtime.block_on(writer.wait_for_capacity(4))?;
                writer.write(encoder.get_ref());
                uploaded += encoder.get_ref().len();
                encoder.get_mut().clear();
            }
        }
        let rest = encoder.finish()?;
        writer.write(&rest);
        uploaded += rest.len();
        Ok(())
    };
    match write() {
        Ok(()) => {
            runtime.block_on(writer.finish())?;
            let labels = UploadLabels {
                db: db_file.display().to_string(),
                kind: "snapshot",
                result: "ok",
            };
            metrics().backup_uploaded_bytes.get_or_create(&labels).inc_by(uploaded as u64);
            Ok(())
        }
        Err(err) => {
            let _ = runtime.block_on(writer.abort());
            Err(err)
        }
    }
}

pub struct Restored {
    pub chain: Vec<String>,
    pub frames: u64,
    pub complete: bool,
}

/// Merges the frame objects of a finished generation into objects of up to
/// [`MAX_SEGMENT_FRAMES`] frames. Frames are uploaded about every second, so
/// a generation consists of hundreds of objects, and a restore through a day
/// of generations would download tens of thousands of them. A restore takes
/// the merged objects and skips the ones they cover; the originals stay for
/// restores to a point in time, which select objects by upload time. Returns
/// the number of merged objects uploaded. Objects are merged one at a time as
/// they arrive, so memory holds the compressed result rather than the frames.
pub async fn compact(target: &Target, db: &str, id: &str) -> Result<usize> {
    let mut runs: Vec<Vec<SegmentObject>> = Vec::new();
    let mut next = 1;
    for segment in target.segments(id, None).await? {
        if segment.last < next {
            continue;
        }
        if segment.first > next {
            break;
        }
        next = segment.last + 1;
        match runs.last_mut() {
            Some(run) if segment.last - run[0].first < MAX_SEGMENT_FRAMES => run.push(segment),
            _ => runs.push(vec![segment]),
        }
    }
    let mut merged = 0;
    for run in runs.into_iter().filter(|run| run.len() > 1) {
        let (first, last) = (run[0].first, run[run.len() - 1].last);
        let mut downloads = futures_util::stream::iter(run)
            .map(|segment| {
                let store = target.store.clone();
                async move { anyhow::Ok(store.get(&segment.location).await?.bytes().await?) }
            })
            .buffered(PARALLEL_DOWNLOADS);
        let mut merger = SegmentMerger::new(first, last)?;
        while let Some(compressed) = downloads.next().await {
            merger
                .append(&compressed?)
                .with_context(|| format!("failed to merge the frames of backup generation {id}"))?;
        }
        let body = merger
            .finish()
            .with_context(|| format!("failed to merge the frames of backup generation {id}"))?;
        let size = body.content_length();
        let location = target
            .generation(id)
            .join("frames")
            .join(segment_name(first, last).as_str());
        let result = target.store.put(&location, body).await;
        let labels = UploadLabels {
            db: db.to_string(),
            kind: "merged",
            result: if result.is_ok() { "ok" } else { "error" },
        };
        metrics().backup_uploads.get_or_create(&labels).inc();
        result?;
        metrics().backup_uploaded_bytes.get_or_create(&labels).inc_by(size as u64);
        merged += 1;
    }
    Ok(merged)
}

/// Joins consecutive, possibly overlapping segments covering frames
/// `first..=last` into one compressed segment, reading each part as a stream.
struct SegmentMerger {
    encoder: zstd::Encoder<'static, Payload>,
    first: u64,
    last: u64,
    next: u64,
    page_size: u32,
}

impl SegmentMerger {
    fn new(first: u64, last: u64) -> Result<Self> {
        let mut encoder = zstd::Encoder::new(Payload(PutPayloadMut::new()), ZSTD_LEVEL)?;
        encoder.include_checksum(true)?;
        Ok(Self { encoder, first, last, next: first, page_size: 0 })
    }

    fn append(&mut self, compressed: &[u8]) -> Result<()> {
        let mut part = zstd::stream::read::Decoder::with_buffer(compressed)?;
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        if part.read_exact(&mut header).is_err() || &header[..8] != SEGMENT_MAGIC {
            bail!("not a WAL segment");
        }
        let size = u32::from_be_bytes(header[8..12].try_into()?);
        let start = u64::from_be_bytes(header[12..20].try_into()?);
        let count = u64::from(u32::from_be_bytes(header[20..24].try_into()?));
        if self.page_size == 0 {
            if start != self.first {
                bail!("the first segment starts at frame {start}, not {}", self.first);
            }
            self.page_size = size;
            header[20..24].copy_from_slice(&u32::try_from(self.last - self.first + 1)?.to_be_bytes());
            self.encoder.write_all(&header)?;
        } else if size != self.page_size {
            bail!("page size {size} differs from {}", self.page_size);
        } else if start > self.next {
            bail!("frames {}..{} are missing", self.next, start - 1);
        }
        let frame_size = (WAL_FRAME_HEADER_SIZE + size as usize) as u64;
        let skip = (self.next - start).min(count);
        let skipped = std::io::copy(&mut (&mut part).take(skip * frame_size), &mut std::io::sink())?;
        let kept = std::io::copy(&mut (&mut part).take((count - skip) * frame_size), &mut self.encoder)?;
        if skipped + kept != count * frame_size || part.read(&mut [0u8])? != 0 {
            bail!("segment {start} does not hold the {count} frames it declares");
        }
        self.next = self.next.max(start + count);
        Ok(())
    }

    fn finish(self) -> Result<PutPayload> {
        if self.next != self.last + 1 {
            bail!("the segments end at frame {}, not {}", self.next - 1, self.last);
        }
        Ok(self.encoder.finish()?.0.freeze())
    }
}

/// Collects an upload body in blocks instead of one growing buffer.
struct Payload(PutPayloadMut);

impl Write for Payload {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Restores the newest backup generation, or the newest one at `at`, into
/// `db_path`, replacing the database and its WAL. Returns `None` when the
/// backup is empty.
pub async fn restore(
    target: &Target,
    db_path: &FsPath,
    at: Option<DateTime<Utc>>,
) -> Result<Option<Restored>> {
    let Some(chain) = restore_chain(target, at).await? else {
        return Ok(None);
    };
    let tmp = PathBuf::from(format!("{}.restore", db_path.display()));
    let mut applier = Applier {
        file: File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("failed to create {}", tmp.display()))?,
        page_size: 0,
        next_frame: 1,
        pending: Vec::new(),
        frames: 0,
    };
    let result = async {
        let listings: Vec<Vec<SegmentObject>> = futures_util::stream::iter(&chain)
            .map(|id| target.segments(id, at))
            .buffered(PARALLEL_DOWNLOADS)
            .try_collect()
            .await?;
        // Overlapping objects come from re-uploads after a crash; a gap ends
        // the restore at the last transaction before it.
        let mut plan = Vec::new();
        let mut complete = true;
        'chain: for (index, segments) in listings.into_iter().enumerate() {
            let mut next = 1;
            for segment in segments {
                if segment.last < next {
                    continue;
                }
                if segment.first > next {
                    warn!(
                        "backup generation {} misses frames {next}..{}, restoring what precedes them",
                        chain[index],
                        segment.first - 1,
                    );
                    complete = false;
                    break 'chain;
                }
                next = segment.last + 1;
                plan.push((index, segment));
            }
        }

        download_snapshot(target, &chain[0], &applier.file).await?;
        applier.read_page_size()?;
        // Merged objects take megabytes each, so the read-ahead is bounded by
        // bytes as well as by count.
        let mut plan = plan.into_iter().peekable();
        let mut downloads = FuturesOrdered::new();
        let mut in_flight = 0;
        let mut generation = None;
        loop {
            while let Some((_, segment)) = plan.peek() {
                if downloads.len() >= PARALLEL_DOWNLOADS
                    || (!downloads.is_empty() && in_flight + segment.size > RESTORE_BUFFER_BYTES)
                {
                    break;
                }
                let (index, segment) = plan.next().unwrap();
                in_flight += segment.size;
                downloads.push_back(async move {
                    let compressed = target.store.get(&segment.location).await?.bytes().await?;
                    anyhow::Ok((index, segment, compressed))
                });
            }
            let Some(download) = downloads.next().await else {
                break;
            };
            let (index, segment, compressed) = download?;
            if generation != Some(index) {
                applier.start_generation();
                generation = Some(index);
            }
            applier
                .apply(zstd::stream::read::Decoder::with_buffer(&compressed[..])?)
                .with_context(|| format!("corrupt backup object {}", segment.location))?;
            in_flight -= segment.size;
        }
        applier.file.sync_all()?;
        anyhow::Ok(complete)
    }
    .await;
    let complete = match result {
        Ok(complete) => complete,
        Err(err) => {
            let _ = fs::remove_file(&tmp);
            return Err(err);
        }
    };
    crash::point("restore.downloaded");
    let continues = (complete && at.is_none())
        .then(|| (chain.last().unwrap().as_str(), chain[0].as_str()));
    install(db_path, &tmp, continues)?;
    Ok(Some(Restored {
        frames: applier.frames,
        chain,
        complete,
    }))
}

/// Generations to replay, oldest (the snapshot) first.
async fn restore_chain(target: &Target, at: Option<DateTime<Utc>>) -> Result<Option<Vec<String>>> {
    let ids = target.list_ids().await?;
    let mut metas = MetaCache {
        target,
        ids: &ids,
        unread: ids.len(),
        known: HashMap::new(),
    };
    let candidates = ids
        .iter()
        .rev()
        .filter(|id| at.is_none_or(|at| id_time(id).is_some_and(|time| time <= at)));
    for tip in candidates {
        let Some(mut meta) = metas.get(tip).await? else {
            continue;
        };
        let mut chain = vec![tip.clone()];
        while let Meta::Parent(parent) = meta {
            if chain.len() > ids.len() {
                bail!("backup generation {tip} has a cyclic history");
            }
            meta = metas.get(&parent).await?.ok_or_else(|| {
                anyhow!("backup generation {parent}, which {tip} builds on, is missing")
            })?;
            chain.push(parent);
        }
        chain.reverse();
        return Ok(Some(chain));
    }
    Ok(None)
}

/// Reads metas newest first, a batch at a time: a chain walks back through
/// hundreds of generations, and one request per step adds up to minutes.
struct MetaCache<'a> {
    target: &'a Target,
    ids: &'a [String],
    /// `ids[..unread]` have not been fetched yet.
    unread: usize,
    known: HashMap<String, Option<Meta>>,
}

impl MetaCache<'_> {
    async fn get(&mut self, id: &str) -> Result<Option<Meta>> {
        while !self.known.contains_key(id) && self.unread > 0 {
            let batch = &self.ids[self.unread.saturating_sub(PARALLEL_DOWNLOADS)..self.unread];
            let metas: Vec<Option<Meta>> = futures_util::stream::iter(batch)
                .map(|id| self.target.meta(id))
                .buffered(PARALLEL_DOWNLOADS)
                .try_collect()
                .await?;
            self.known.extend(batch.iter().cloned().zip(metas));
            self.unread -= batch.len();
        }
        match self.known.get(id) {
            Some(meta) => Ok(meta.clone()),
            None => self.target.meta(id).await,
        }
    }
}

async fn download_snapshot(target: &Target, id: &str, file: &File) -> Result<()> {
    let location = target.generation(id).join("snapshot.zst");
    let mut stream = target.store.get(&location).await?.into_stream();
    let mut decoder = zstd::stream::write::Decoder::new(file)?;
    while let Some(chunk) = stream.next().await {
        decoder.write_all(&chunk?)?;
    }
    decoder.flush()?;
    Ok(())
}

/// Replays WAL frames onto a database file the way a checkpoint does, one
/// transaction at a time.
struct Applier {
    file: File,
    page_size: u32,
    next_frame: u64,
    pending: Vec<(u32, Vec<u8>)>,
    frames: u64,
}

impl Applier {
    fn read_page_size(&mut self) -> Result<()> {
        let mut header = [0u8; 18];
        if self.file.metadata()?.len() >= header.len() as u64 {
            self.file.read_exact_at(&mut header, 0)?;
            self.page_size = match u16::from_be_bytes([header[16], header[17]]) {
                1 => 65536,
                size => u32::from(size),
            };
        }
        Ok(())
    }

    /// Frames of an unfinished transaction never reach the file.
    fn start_generation(&mut self) {
        self.next_frame = 1;
        self.pending.clear();
    }

    fn apply(&mut self, mut segment: impl Read) -> Result<()> {
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        if segment.read_exact(&mut header).is_err() || &header[..8] != SEGMENT_MAGIC {
            bail!("not a WAL segment");
        }
        let page_size = u32::from_be_bytes(header[8..12].try_into()?);
        let first = u64::from_be_bytes(header[12..20].try_into()?);
        let count = u64::from(u32::from_be_bytes(header[20..24].try_into()?));
        if self.page_size == 0 {
            self.page_size = page_size;
        } else if self.page_size != page_size {
            bail!("page size {page_size} differs from the database's {}", self.page_size);
        }
        for frame_no in first..first + count {
            let mut frame = [0u8; WAL_FRAME_HEADER_SIZE];
            let mut page = vec![0u8; page_size as usize];
            if segment.read_exact(&mut frame).and_then(|_| segment.read_exact(&mut page)).is_err() {
                bail!("expected {count} frames of {page_size}-byte pages, got {}", frame_no - first);
            }
            if frame_no < self.next_frame {
                continue;
            }
            let page_no = u32::from_be_bytes(frame[0..4].try_into()?);
            let db_size = u32::from_be_bytes(frame[4..8].try_into()?);
            if page_no == 0 {
                bail!("frame {frame_no} has no page number");
            }
            self.pending.push((page_no, page));
            self.next_frame = frame_no + 1;
            if db_size != 0 {
                let page_size = u64::from(self.page_size);
                for (page_no, page) in self.pending.drain(..) {
                    self.file.write_all_at(&page, (u64::from(page_no) - 1) * page_size)?;
                }
                self.file.set_len(u64::from(db_size) * page_size)?;
            }
            self.frames += 1;
        }
        if segment.read(&mut [0u8])? != 0 {
            bail!("segment {first} holds more than the {count} frames it declares");
        }
        Ok(())
    }
}

/// Moves the restored file into place and drops the WAL and sync state that
/// belonged to the replaced database. `continues` lets the next backup
/// generation build on the restored one instead of taking a snapshot.
fn install(db_path: &FsPath, tmp: &FsPath, continues: Option<(&str, &str)>) -> Result<()> {
    let db = db_path.display().to_string();
    for suffix in ["-wal", "-shm"] {
        match fs::remove_file(format!("{db}{suffix}")) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err.into()),
            _ => {}
        }
    }
    let sync_dir = PathBuf::from(format!("{db}-sync"));
    if sync_dir.exists() {
        fs::remove_dir_all(&sync_dir)?;
    }
    fs::rename(tmp, db_path)?;
    if let Some(parent) = db_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)?.sync_all()?;
    }
    crash::point("restore.installed");
    if let Some((tip, root)) = continues {
        fs::create_dir_all(&sync_dir)?;
        write_atomic(&sync_dir.join(RESTORED_FILE), format!("{tip} {root}\n").as_bytes())?;
    }
    Ok(())
}

/// Restores `db_path` from the backup if the file does not exist.
pub async fn restore_missing(target: &Target, db_path: &FsPath) -> Result<()> {
    if db_path.exists() {
        return Ok(());
    }
    match restore(target, db_path, None).await? {
        Some(restored) => info!(
            "{}: restored from {target}, generation {} ({} generations, {} frames replayed)",
            db_path.display(),
            restored.chain.last().unwrap(),
            restored.chain.len(),
            restored.frames,
        ),
        None => info!("{}: no backup in {target}, starting empty", db_path.display()),
    }
    Ok(())
}

pub struct GenerationInfo {
    pub id: String,
    pub meta: Option<Meta>,
    pub last_frame: u64,
    pub last_upload: Option<DateTime<Utc>>,
}

pub async fn list(target: &Target) -> Result<Vec<GenerationInfo>> {
    let mut generations = Vec::new();
    for id in target.list_ids().await? {
        let segments = target.segments(&id, None).await?;
        generations.push(GenerationInfo {
            meta: target.meta(&id).await?,
            last_frame: segments.iter().map(|s| s.last).max().unwrap_or(0),
            last_upload: segments.iter().map(|s| s.uploaded).max(),
            id,
        });
    }
    Ok(generations)
}

pub fn generation_time(id: &str) -> Option<DateTime<Utc>> {
    id_time(id)
}

/// Removes generations needed only to restore points older than `retention`:
/// everything before the newest snapshot taken before that point, unless a
/// later generation still builds on it.
pub async fn prune(target: &Target, retention: Duration) -> Result<usize> {
    let cutoff = Utc::now() - TimeDelta::from_std(retention)?;
    let ids = target.list_ids().await?;
    let mut metas = BTreeMap::new();
    let mut keep_from = None;
    for id in ids.iter().rev() {
        if id_time(id).is_none_or(|time| time > cutoff) {
            continue;
        }
        let meta = target.meta(id).await?;
        let is_snapshot = meta == Some(Meta::Snapshot);
        metas.insert(id.clone(), meta);
        if is_snapshot {
            keep_from = Some(id.clone());
            break;
        }
    }
    let Some(keep_from) = keep_from else {
        return Ok(0);
    };
    let mut needed = HashSet::new();
    for id in ids.iter().filter(|id| **id >= keep_from) {
        let mut current = id.clone();
        loop {
            let meta = match metas.get(&current) {
                Some(meta) => meta.clone(),
                None => {
                    let meta = target.meta(&current).await?;
                    metas.insert(current.clone(), meta.clone());
                    meta
                }
            };
            match meta {
                Some(Meta::Parent(parent)) if parent < keep_from && needed.insert(parent.clone()) => {
                    current = parent;
                }
                _ => break,
            }
        }
    }
    let mut removed = 0;
    for id in ids.iter().filter(|id| **id < keep_from && !needed.contains(*id)) {
        target.remove_generation(id).await?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn ids_sort_by_time() {
        let first = new_id(0xabcdef01, 7, None);
        assert!(first.ends_with("Z-abcdef01-7"), "{first}");
        let time = id_time(&first).unwrap();
        assert!((Utc::now() - time).num_seconds().abs() < 5);

        let future = format!("{}Z-00000000-1", (Utc::now() + TimeDelta::hours(1)).format(ID_TIME_FORMAT));
        let next = new_id(0, 2, Some(&future));
        assert!(next > future, "{next} must sort after {future}");
        assert_eq!(id_time(&next).unwrap() - id_time(&future).unwrap(), TimeDelta::milliseconds(1));
    }

    #[test]
    fn state_roundtrips() {
        let state = State {
            epoch: 0xdeadbeef,
            generation: 12,
            id: Some("20261017T120000000Z-deadbeef-12".to_string()),
            meta: Some(Meta::Parent("20261017T110000000Z-deadbeef-11".to_string())),
            established: true,
            shipped: 42,
            parent: None,
            last_id: Some("20261017T120000000Z-deadbeef-12".to_string()),
            snapshot_at: 1_790_000_000,
        };
        assert_eq!(State::decode(&state.encode()).unwrap(), state);
        assert_eq!(State::decode(&State::default().encode()).unwrap(), State::default());
    }

    #[test]
    fn parses_segment_names() {
        assert_eq!(parse_segment_name(&segment_name(1, 4096)), Some((1, 4096)));
        assert_eq!(parse_segment_name("000000000005-000000000004.zst"), None);
        assert_eq!(parse_segment_name("000000000000-000000000004.zst"), None);
        assert_eq!(parse_segment_name("meta"), None);
    }

    fn frame(page_no: u32, db_size: u32, fill: u8, page_size: usize) -> Vec<u8> {
        let mut frame = vec![0u8; WAL_FRAME_HEADER_SIZE];
        frame[0..4].copy_from_slice(&page_no.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        frame.extend(std::iter::repeat_n(fill, page_size));
        frame
    }

    fn segment(first: u64, frames: &[Vec<u8>], page_size: u32) -> Vec<u8> {
        let mut bytes = SEGMENT_MAGIC.to_vec();
        bytes.extend_from_slice(&page_size.to_be_bytes());
        bytes.extend_from_slice(&first.to_be_bytes());
        bytes.extend_from_slice(&(frames.len() as u32).to_be_bytes());
        for frame in frames {
            bytes.extend_from_slice(frame);
        }
        bytes
    }

    fn merge(first: u64, last: u64, parts: &[&[u8]]) -> Result<Vec<u8>> {
        let mut merger = SegmentMerger::new(first, last)?;
        for part in parts {
            merger.append(&compress(part)?)?;
        }
        let body: Bytes = merger.finish()?.into();
        Ok(zstd::decode_all(&body[..])?)
    }

    #[test]
    fn merges_overlapping_segments_and_refuses_gaps() {
        let frames: Vec<Vec<u8>> = (1..=5).map(|n| frame(n, n, n as u8, 512)).collect();
        let parts: [&[u8]; 2] = [&segment(1, &frames[..3], 512), &segment(2, &frames[1..], 512)];
        assert_eq!(merge(1, 5, &parts).unwrap(), segment(1, &frames, 512));
        let covered: [&[u8]; 2] = [&segment(1, &frames, 512), &segment(2, &frames[1..3], 512)];
        assert_eq!(merge(1, 5, &covered).unwrap(), segment(1, &frames, 512));

        let parts: [&[u8]; 2] = [&segment(1, &frames[..2], 512), &segment(4, &frames[3..], 512)];
        assert!(merge(1, 5, &parts).is_err());
    }

    #[test]
    fn refuses_segments_that_do_not_match_their_frames() {
        let frames: Vec<Vec<u8>> = (1..=4).map(|n| frame(n, n, n as u8, 512)).collect();
        let whole = segment(1, &frames, 512);
        assert!(merge(1, 4, &[&whole[..whole.len() - 1]]).is_err());
        assert!(merge(1, 4, &[&[&whole[..], &[0]].concat()]).is_err());
        assert!(merge(1, 3, &[&whole]).is_err());
        assert!(merge(2, 4, &[&whole]).is_err());
        assert!(merge(1, 4, &[&segment(1, &frames[..2], 512), &segment(3, &frames[2..], 1024)]).is_err());
    }

    #[tokio::test]
    async fn an_unreadable_state_file_starts_the_backup_over() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = Target::open(dir.path().join("backup").to_str().unwrap()).unwrap();
        let db = dir.path().join("db").to_string_lossy().to_string();
        fs::create_dir_all(format!("{db}-sync")).unwrap();
        fs::write(format!("{db}-sync/{STATE_FILE}"), b"epoch 1\ngener").unwrap();

        let replica = Replica::open(target, &db, 7, 3, Duration::from_secs(3600)).unwrap();
        assert!(replica.snapshot_pending());
        assert_eq!((replica.state.epoch, replica.state.generation), (7, 3));
        let stored = fs::read_to_string(format!("{db}-sync/{STATE_FILE}")).unwrap();
        assert_eq!(State::decode(&stored).unwrap(), replica.state);
    }

    #[tokio::test]
    async fn prunes_only_history_no_generation_needs() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = Target::open(dir.path().to_str().unwrap()).unwrap();
        let days_ago = |days: i64| {
            let time = Utc::now() - TimeDelta::days(days);
            format!("{}Z-00000001-{days}", time.format(ID_TIME_FORMAT))
        };
        let (s40, p39, s35, p34, s1, p0) =
            (days_ago(40), days_ago(39), days_ago(35), days_ago(34), days_ago(1), days_ago(0));
        let metas = [
            (&s40, Meta::Snapshot),
            (&p39, Meta::Parent(s40.clone())),
            (&s35, Meta::Snapshot),
            (&p34, Meta::Parent(s35.clone())),
            (&s1, Meta::Snapshot),
            // A late branch off an old generation keeps that history alive.
            (&p0, Meta::Parent(p39.clone())),
        ];
        for (id, meta) in &metas {
            let location = target.generation(id).join("meta");
            target.store.put(&location, meta.encode().into()).await.unwrap();
        }

        assert_eq!(prune(&target, Duration::from_secs(30 * 24 * 3600)).await.unwrap(), 0);
        target.remove_generation(&p0).await.unwrap();
        assert_eq!(prune(&target, Duration::from_secs(30 * 24 * 3600)).await.unwrap(), 2);
        assert_eq!(target.list_ids().await.unwrap(), vec![s35, p34, s1]);
    }

    #[tokio::test]
    async fn removes_every_object_of_a_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = Target::open(dir.path().to_str().unwrap()).unwrap();
        for id in ["a", "b"] {
            target.store.put(&target.generation(id).join("meta"), Meta::Snapshot.encode().into()).await.unwrap();
            for n in 0..1500 {
                let location = target.generation(id).join("frames").join(format!("{n:08}"));
                target.store.put(&location, vec![0u8; 8].into()).await.unwrap();
            }
        }
        target.remove_generation("a").await.unwrap();
        assert_eq!(target.list_ids().await.unwrap(), vec!["b".to_string()]);
        assert_eq!(target.store.list(Some(&target.generation("b"))).count().await, 1501);
    }

    #[test]
    fn applies_only_committed_frames() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let mut applier = Applier {
            file: File::options().create(true).truncate(true).read(true).write(true).open(&path).unwrap(),
            page_size: 0,
            next_frame: 1,
            pending: Vec::new(),
            frames: 0,
        };
        applier.read_page_size().unwrap();
        applier.start_generation();
        applier
            .apply(&segment(1, &[frame(1, 0, 1, 512), frame(2, 2, 2, 512), frame(1, 0, 3, 512)], 512)[..])
            .unwrap();
        // Frame 3 overlaps the next segment, frame 4 commits a shrink to one page.
        applier
            .apply(&segment(3, &[frame(1, 0, 3, 512), frame(1, 1, 4, 512), frame(2, 0, 9, 512)], 512)[..])
            .unwrap();
        applier.start_generation();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 512);
        assert!(bytes.iter().all(|b| *b == 4));
        assert_eq!(applier.next_frame, 1);
        assert!(applier
            .apply(&segment(1, &[frame(1, 1, 0, 1024)], 1024)[..])
            .is_err());
        let whole = segment(5, &[frame(1, 0, 5, 512)], 512);
        assert!(applier.apply(&whole[..whole.len() - 1]).is_err());
        assert!(applier.apply(&[&whole[..], &[0]].concat()[..]).is_err());
    }
}
