//! Prometheus metrics, served on `--metrics-listen`.
//!
//! Counters are updated where things happen. Database gauges are refreshed
//! from the database state on every scrape, process metrics are read then too.

use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::time::SystemTime;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric};
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DbLabels {
    pub db: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CheckpointLabels {
    pub db: String,
    pub outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct UploadLabels {
    pub db: String,
    pub kind: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    pub endpoint: &'static str,
    pub status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EndpointLabels {
    pub endpoint: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    version: &'static str,
}

type FloatGauge = Gauge<f64, AtomicU64>;

pub struct Metrics {
    registry: Registry,
    pub requests: Family<RequestLabels, Counter>,
    pub request_duration: Family<EndpointLabels, Histogram>,
    pub checkpoints: Family<CheckpointLabels, Counter>,
    pub backup_uploads: Family<UploadLabels, Counter>,
    pub backup_uploaded_bytes: Family<UploadLabels, Counter>,
    pub open_databases: Gauge,
    pub wal_frames: Family<DbLabels, Gauge>,
    pub generation: Family<DbLabels, Gauge>,
    pub last_checkpoint: Family<DbLabels, FloatGauge>,
    pub backup_pending_frames: Family<DbLabels, Gauge>,
    pub backup_snapshot_pending: Family<DbLabels, Gauge>,
    pub backup_synced: Family<DbLabels, FloatGauge>,
    pub backup_last_snapshot: Family<DbLabels, FloatGauge>,
}

pub fn metrics() -> &'static Metrics {
    static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);
    &METRICS
}

impl Metrics {
    fn new() -> Self {
        let mut root = Registry::default();
        root.register_collector(Box::new(ProcessCollector));
        let registry = root.sub_registry_with_prefix("turso_sync");
        let info = Family::<InfoLabels, Gauge>::default();
        info.get_or_create(&InfoLabels {
            version: env!("CARGO_PKG_VERSION"),
        })
        .set(1);
        registry.register("build_info", "Server version", info);

        let metrics = Self {
            requests: Family::default(),
            request_duration: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 15))
            }),
            checkpoints: Family::default(),
            backup_uploads: Family::default(),
            backup_uploaded_bytes: Family::default(),
            open_databases: Gauge::default(),
            wal_frames: Family::default(),
            generation: Family::default(),
            last_checkpoint: Family::default(),
            backup_pending_frames: Family::default(),
            backup_snapshot_pending: Family::default(),
            backup_synced: Family::default(),
            backup_last_snapshot: Family::default(),
            registry: Registry::default(),
        };
        registry.register(
            "http_requests",
            "HTTP requests by endpoint and status",
            metrics.requests.clone(),
        );
        registry.register(
            "http_request_duration_seconds",
            "Time to answer HTTP requests",
            metrics.request_duration.clone(),
        );
        registry.register(
            "checkpoints",
            "Checkpoints by outcome: done, deferred (held off by a reader or the backup) or failed",
            metrics.checkpoints.clone(),
        );
        registry.register(
            "backup_uploads",
            "Backup uploads by kind (snapshot, frames) and result",
            metrics.backup_uploads.clone(),
        );
        registry.register(
            "backup_uploaded_bytes",
            "Compressed bytes uploaded to the backup",
            metrics.backup_uploaded_bytes.clone(),
        );
        registry.register(
            "open_databases",
            "Databases held open",
            metrics.open_databases.clone(),
        );
        registry.register(
            "wal_frames",
            "Frames in the WAL of the current generation",
            metrics.wal_frames.clone(),
        );
        registry.register(
            "generation",
            "Current checkpoint generation",
            metrics.generation.clone(),
        );
        registry.register(
            "last_checkpoint_timestamp_seconds",
            "When the WAL was last checkpointed",
            metrics.last_checkpoint.clone(),
        );
        registry.register(
            "backup_pending_frames",
            "Committed WAL frames not in the backup yet",
            metrics.backup_pending_frames.clone(),
        );
        registry.register(
            "backup_snapshot_pending",
            "1 while the backup waits for a snapshot to become restorable again",
            metrics.backup_snapshot_pending.clone(),
        );
        registry.register(
            "backup_synced_timestamp_seconds",
            "When the backup last held every committed change",
            metrics.backup_synced.clone(),
        );
        registry.register(
            "backup_last_snapshot_timestamp_seconds",
            "When the backup chain last started from a snapshot",
            metrics.backup_last_snapshot.clone(),
        );
        Self {
            registry: root,
            ..metrics
        }
    }

    pub fn encode(&self) -> String {
        let mut text = String::new();
        encode(&mut text, &self.registry).expect("writing to a String cannot fail");
        text
    }
}

/// The standard `process_*` metrics. Memory and descriptors are read from
/// `/proc`, so they are missing outside Linux.
#[derive(Debug)]
struct ProcessCollector;

impl Collector for ProcessCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let cpu = ConstCounter::new(cpu_seconds());
        cpu.encode(encoder.encode_descriptor(
            "process_cpu_seconds",
            "User and system CPU time spent",
            None,
            cpu.metric_type(),
        )?)?;
        if let Some(bytes) = resident_memory_bytes() {
            let rss = ConstGauge::new(bytes);
            rss.encode(encoder.encode_descriptor(
                "process_resident_memory_bytes",
                "Resident memory size",
                None,
                rss.metric_type(),
            )?)?;
        }
        if let Some(count) = open_fds() {
            let fds = ConstGauge::new(count);
            fds.encode(encoder.encode_descriptor(
                "process_open_fds",
                "Open file descriptors",
                None,
                fds.metric_type(),
            )?)?;
        }
        Ok(())
    }
}

fn cpu_seconds() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

fn resident_memory_bytes() -> Option<i64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: i64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as i64)
}

fn open_fds() -> Option<i64> {
    // Reading the directory takes a descriptor of its own.
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count() as i64 - 1)
}

pub fn db(path: &str) -> DbLabels {
    DbLabels {
        db: path.to_string(),
    }
}

pub fn unix_seconds(time: SystemTime) -> f64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Groups request paths by API endpoint, so labels stay bounded.
pub fn endpoint(path: &str) -> &'static str {
    if path.ends_with("/v2/pipeline") {
        "pipeline"
    } else if path.ends_with("/pull-updates") {
        "pull_updates"
    } else if path.ends_with("/admin/checkpoint") {
        "checkpoint"
    } else if path == "/health" {
        "health"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_registered_metrics() {
        let metrics = metrics();
        metrics
            .requests
            .get_or_create(&RequestLabels {
                endpoint: endpoint("/db/a/v2/pipeline"),
                status: 200,
            })
            .inc();
        metrics.wal_frames.get_or_create(&db("/data/main.db")).set(7);
        let text = metrics.encode();
        assert!(text.contains(r#"turso_sync_http_requests_total{endpoint="pipeline",status="200"} 1"#), "{text}");
        assert!(text.contains(r#"turso_sync_wal_frames{db="/data/main.db"} 7"#), "{text}");
        assert!(text.contains("turso_sync_build_info{version="), "{text}");
        assert!(text.contains("\nprocess_cpu_seconds_total "), "{text}");
    }
}
