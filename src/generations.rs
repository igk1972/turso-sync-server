//! Generations make WAL checkpoints safe for sync clients.
//!
//! A client revision points at a WAL frame. A checkpoint truncates the WAL and
//! restarts frame numbering, so every checkpoint starts a new generation and
//! revisions carry it: `{epoch}:{generation}:{frame}`. Before the WAL of a
//! generation is checkpointed, the set of pages it changed is sealed on disk;
//! a client from an older generation is then sent every page sealed since its
//! generation plus the pages of the current WAL, read at their latest version.
//!
//! The database file is never written within a generation (automatic
//! checkpoints are disabled), which lets startup detect a WAL reset or a
//! database file change made while the server was down.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{anyhow, Context, Result};
use roaring::RoaringBitmap;

const PAGES_SUFFIX: &str = ".pages";
const UNKNOWN_SUFFIX: &str = ".unknown";
const WAL_MAGIC_BE: u32 = 0x377f0682;
const WAL_MAGIC_LE: u32 = 0x377f0683;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revision {
    Empty,
    /// A bare frame number, as issued by `tursodb --sync-server`.
    Legacy(u64),
    Generation { epoch: u32, generation: u64, frame: u64 },
    Invalid,
}

impl Revision {
    pub fn parse(revision: &str) -> Self {
        if revision.is_empty() {
            return Self::Empty;
        }
        if let Ok(frame) = revision.parse::<u64>() {
            return Self::Legacy(frame);
        }
        let mut parts = revision.split(':');
        let (Some(epoch), Some(generation), Some(frame), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Self::Invalid;
        };
        match (
            u32::from_str_radix(epoch, 16),
            generation.parse::<u64>(),
            frame.parse::<u64>(),
        ) {
            (Ok(epoch), Ok(generation), Ok(frame)) => Self::Generation {
                epoch,
                generation,
                frame,
            },
            _ => Self::Invalid,
        }
    }

    pub fn format(epoch: u32, generation: u64, frame: u64) -> String {
        format!("{epoch:08x}:{generation}:{frame}")
    }
}

/// What a sealed generation changed. `Unknown` marks a WAL that was reset
/// behind the server's back: its pages are lost, so clients from that
/// generation or earlier must receive the whole database.
#[derive(Debug, Clone, PartialEq)]
pub enum SealedPages {
    Known(RoaringBitmap),
    Unknown,
}

/// The state the current generation started from, used at startup to detect
/// changes made while the server was not running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalIdentity {
    pub generation: u64,
    pub checkpoint_seq: u32,
    /// `None` while the WAL of the generation has no frames yet.
    pub salts: Option<(u32, u32)>,
    pub db_len: u64,
    pub db_mtime_nanos: u128,
}

impl WalIdentity {
    fn encode(&self) -> String {
        let salts = match self.salts {
            Some((s1, s2)) => format!("{s1} {s2}"),
            None => "- -".to_string(),
        };
        format!(
            "{} {} {} {} {}\n",
            self.generation, self.checkpoint_seq, salts, self.db_len, self.db_mtime_nanos
        )
    }

    fn decode(text: &str) -> Result<Self> {
        let fields: Vec<&str> = text.split_whitespace().collect();
        let [generation, seq, s1, s2, db_len, mtime] = fields[..] else {
            return Err(anyhow!("malformed WAL identity: {text:?}"));
        };
        let salts = match (s1, s2) {
            ("-", "-") => None,
            _ => Some((s1.parse()?, s2.parse()?)),
        };
        Ok(Self {
            generation: generation.parse()?,
            checkpoint_seq: seq.parse()?,
            salts,
            db_len: db_len.parse()?,
            db_mtime_nanos: mtime.parse()?,
        })
    }
}

/// Checkpoint sequence number and salts from the header of `{db}-wal`.
pub fn read_wal_header(db_path: &str) -> Option<(u32, u32, u32)> {
    let mut header = [0u8; 32];
    File::open(format!("{db_path}-wal"))
        .and_then(|mut file| file.read_exact(&mut header))
        .ok()?;
    let word = |offset: usize| u32::from_be_bytes(header[offset..offset + 4].try_into().unwrap());
    if word(0) != WAL_MAGIC_BE && word(0) != WAL_MAGIC_LE {
        return None;
    }
    Some((word(12), word(16), word(20)))
}

fn db_file_meta(db_path: &str) -> (u64, u128) {
    match fs::metadata(db_path) {
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_nanos());
            (meta.len(), mtime)
        }
        Err(_) => (0, 0),
    }
}

pub struct Generations {
    /// `None` keeps everything in memory (in-memory databases, read-only mode).
    dir: Option<PathBuf>,
    db_path: String,
    epoch: u32,
    current: u64,
    sealed_in_memory: BTreeMap<u64, SealedPages>,
    keep: usize,
}

impl Generations {
    pub fn open(db_path: &str, persistent: bool, keep: usize) -> Result<Self> {
        if !persistent {
            return Ok(Self {
                dir: None,
                db_path: db_path.to_string(),
                epoch: rand_epoch(),
                current: 0,
                sealed_in_memory: BTreeMap::new(),
                keep,
            });
        }
        let dir = PathBuf::from(format!("{db_path}-sync"));
        fs::create_dir_all(dir.join("generations"))
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let epoch_path = dir.join("epoch");
        let epoch = match fs::read_to_string(&epoch_path) {
            Ok(text) => u32::from_str_radix(text.trim(), 16)
                .with_context(|| format!("malformed {}", epoch_path.display()))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let epoch = rand_epoch();
                write_atomic(&epoch_path, format!("{epoch:08x}\n").as_bytes())?;
                epoch
            }
            Err(err) => return Err(err.into()),
        };
        let current = list_generations(&dir)?
            .last()
            .map_or(0, |(generation, _)| generation + 1);
        Ok(Self {
            dir: Some(dir),
            db_path: db_path.to_string(),
            epoch,
            current,
            sealed_in_memory: BTreeMap::new(),
            keep,
        })
    }

    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    pub fn current(&self) -> u64 {
        self.current
    }

    pub fn revision(&self, frame: u64) -> String {
        Revision::format(self.epoch, self.current, frame)
    }

    /// Durably records what the current generation changed and moves on to
    /// the next one. Must happen before the WAL of the generation is reset.
    pub fn seal(&mut self, pages: SealedPages) -> Result<()> {
        let generation = self.current;
        match &self.dir {
            Some(dir) => {
                let generations = dir.join("generations");
                match &pages {
                    SealedPages::Known(bitmap) => {
                        let mut bytes = Vec::with_capacity(bitmap.serialized_size());
                        bitmap.serialize_into(&mut bytes)?;
                        write_atomic(
                            &generations.join(format!("{generation}{PAGES_SUFFIX}")),
                            &bytes,
                        )?;
                    }
                    SealedPages::Unknown => write_atomic(
                        &generations.join(format!("{generation}{UNKNOWN_SUFFIX}")),
                        b"",
                    )?,
                }
            }
            None => {
                self.sealed_in_memory.insert(generation, pages);
            }
        }
        self.current += 1;
        self.prune()
    }

    /// Pages changed from the start of `generation` up to the start of the
    /// current one, or `None` if that history is not fully known.
    pub fn changed_since(&self, generation: u64) -> Result<Option<RoaringBitmap>> {
        let mut changed = RoaringBitmap::new();
        for g in generation..self.current {
            match self.load(g)? {
                Some(SealedPages::Known(bitmap)) => changed |= bitmap,
                Some(SealedPages::Unknown) | None => return Ok(None),
            }
        }
        Ok(Some(changed))
    }

    fn load(&self, generation: u64) -> Result<Option<SealedPages>> {
        let Some(dir) = &self.dir else {
            return Ok(self.sealed_in_memory.get(&generation).cloned());
        };
        let generations = dir.join("generations");
        if generations
            .join(format!("{generation}{UNKNOWN_SUFFIX}"))
            .exists()
        {
            return Ok(Some(SealedPages::Unknown));
        }
        match fs::read(generations.join(format!("{generation}{PAGES_SUFFIX}"))) {
            Ok(bytes) => Ok(Some(SealedPages::Known(
                RoaringBitmap::deserialize_from(&bytes[..])
                    .with_context(|| format!("corrupt page set of generation {generation}"))?,
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn prune(&mut self) -> Result<()> {
        let Some(dir) = &self.dir else {
            while self.sealed_in_memory.len() > self.keep {
                self.sealed_in_memory.pop_first();
            }
            return Ok(());
        };
        let sealed = list_generations(dir)?;
        let excess = sealed.len().saturating_sub(self.keep);
        // The newest sealed generation is what the current one is derived from
        // on restart, so it is never removed.
        for (_, path) in sealed.iter().take(excess.min(sealed.len().saturating_sub(1))) {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    pub fn load_identity(&self) -> Result<Option<WalIdentity>> {
        let Some(dir) = &self.dir else {
            return Ok(None);
        };
        match fs::read_to_string(dir.join("wal-identity")) {
            Ok(text) => Ok(Some(WalIdentity::decode(&text)?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Records the on-disk state of the current generation.
    pub fn store_identity(&self, checkpoint_seq: u32, wal_has_frames: bool) -> Result<WalIdentity> {
        let salts = if wal_has_frames {
            read_wal_header(&self.db_path).map(|(_, s1, s2)| (s1, s2))
        } else {
            None
        };
        let (db_len, db_mtime_nanos) = db_file_meta(&self.db_path);
        let identity = WalIdentity {
            generation: self.current,
            checkpoint_seq,
            salts,
            db_len,
            db_mtime_nanos,
        };
        if let Some(dir) = &self.dir {
            write_atomic(&dir.join("wal-identity"), identity.encode().as_bytes())?;
        }
        Ok(identity)
    }

    /// Compares the files with the identity recorded for the current
    /// generation. Returns a reason if they changed while the server was down.
    pub fn detect_offline_change(&self, identity: &WalIdentity) -> Option<String> {
        if identity.generation != self.current {
            return None;
        }
        let (db_len, db_mtime_nanos) = db_file_meta(&self.db_path);
        if (db_len, db_mtime_nanos) != (identity.db_len, identity.db_mtime_nanos) {
            return Some("the database file changed".to_string());
        }
        match (read_wal_header(&self.db_path), identity.salts) {
            (None, Some(_)) => Some("the WAL lost its frames".to_string()),
            (Some((seq, _, _)), None) if seq != identity.checkpoint_seq => {
                Some("the WAL was restarted".to_string())
            }
            (Some((seq, s1, s2)), Some(salts))
                if seq != identity.checkpoint_seq || (s1, s2) != salts =>
            {
                Some("the WAL was restarted".to_string())
            }
            _ => None,
        }
    }
}

fn list_generations(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut sealed = Vec::new();
    for entry in fs::read_dir(dir.join("generations"))? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let number = name
            .strip_suffix(PAGES_SUFFIX)
            .or_else(|| name.strip_suffix(UNKNOWN_SUFFIX));
        if let Some(generation) = number.and_then(|n| n.parse::<u64>().ok()) {
            sealed.push((generation, path));
        }
    }
    sealed.sort();
    Ok(sealed)
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn rand_epoch() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(pages: &[u32]) -> RoaringBitmap {
        pages.iter().copied().collect()
    }

    #[test]
    fn parses_revisions() {
        assert_eq!(Revision::parse(""), Revision::Empty);
        assert_eq!(Revision::parse("42"), Revision::Legacy(42));
        assert_eq!(
            Revision::parse(&Revision::format(0xdeadbeef, 3, 17)),
            Revision::Generation {
                epoch: 0xdeadbeef,
                generation: 3,
                frame: 17
            }
        );
        for bad in ["g1:o5", "1:2", "zz:1:2", "a:1:2:3", "a:x:2"] {
            assert_eq!(Revision::parse(bad), Revision::Invalid, "{bad}");
        }
    }

    #[test]
    fn seals_generations_and_reports_changes() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("db").to_string_lossy().to_string();
        let mut gens = Generations::open(&db, true, 10).unwrap();
        assert_eq!(gens.current(), 0);
        gens.seal(SealedPages::Known(bitmap(&[1, 2]))).unwrap();
        gens.seal(SealedPages::Known(bitmap(&[2, 7]))).unwrap();
        assert_eq!(gens.changed_since(0).unwrap(), Some(bitmap(&[1, 2, 7])));
        assert_eq!(gens.changed_since(1).unwrap(), Some(bitmap(&[2, 7])));
        assert_eq!(gens.changed_since(2).unwrap(), Some(RoaringBitmap::new()));

        let reopened = Generations::open(&db, true, 10).unwrap();
        assert_eq!(reopened.current(), 2);
        assert_eq!(reopened.epoch(), gens.epoch());

        gens.seal(SealedPages::Unknown).unwrap();
        gens.seal(SealedPages::Known(bitmap(&[3]))).unwrap();
        assert_eq!(gens.changed_since(1).unwrap(), None);
        assert_eq!(gens.changed_since(3).unwrap(), Some(bitmap(&[3])));
    }

    #[test]
    fn pruned_history_is_unknown() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("db").to_string_lossy().to_string();
        let mut gens = Generations::open(&db, true, 2).unwrap();
        for page in 1..=4 {
            gens.seal(SealedPages::Known(bitmap(&[page]))).unwrap();
        }
        assert_eq!(gens.changed_since(0).unwrap(), None);
        assert_eq!(gens.changed_since(2).unwrap(), Some(bitmap(&[3, 4])));
        assert_eq!(Generations::open(&db, true, 2).unwrap().current(), 4);
    }

    #[test]
    fn in_memory_generations_start_fresh() {
        let mut gens = Generations::open(":memory:", false, 10).unwrap();
        gens.seal(SealedPages::Known(bitmap(&[5]))).unwrap();
        assert_eq!(gens.changed_since(0).unwrap(), Some(bitmap(&[5])));
        assert_eq!(Generations::open(":memory:", false, 10).unwrap().current(), 0);
    }

    #[test]
    fn identity_roundtrips() {
        for salts in [None, Some((1, 2))] {
            let identity = WalIdentity {
                generation: 4,
                checkpoint_seq: 9,
                salts,
                db_len: 8192,
                db_mtime_nanos: 1_700_000_000_123_456_789,
            };
            assert_eq!(WalIdentity::decode(&identity.encode()).unwrap(), identity);
        }
    }

    #[test]
    fn detects_offline_database_changes() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("db").to_string_lossy().to_string();
        fs::write(&db, vec![0u8; 4096]).unwrap();
        let gens = Generations::open(&db, true, 10).unwrap();
        let identity = gens.store_identity(0, false).unwrap();
        assert_eq!(gens.detect_offline_change(&identity), None);

        fs::write(&db, vec![1u8; 8192]).unwrap();
        assert!(gens.detect_offline_change(&identity).is_some());
    }
}
