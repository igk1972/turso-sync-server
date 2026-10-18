//! Crash tests: `TURSO_SYNC_CRASH_AT=name[:n]` kills the process with SIGKILL
//! the n-th time (the first by default) it passes the named point.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

static CRASH_AT: LazyLock<Option<(String, AtomicU64)>> = LazyLock::new(|| {
    let spec = std::env::var("TURSO_SYNC_CRASH_AT").ok()?;
    let (name, nth) = match spec.split_once(':') {
        Some((name, nth)) => (name, nth.parse().ok()?),
        None => (spec.as_str(), 1),
    };
    Some((name.to_string(), AtomicU64::new(nth)))
});

pub fn point(name: &str) {
    let Some((target, remaining)) = CRASH_AT.as_ref() else {
        return;
    };
    if target == name && remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
        eprintln!("crash point {name}");
        unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
        // The signal can land after kill() returns; nothing may run meanwhile.
        loop {
            std::thread::park();
        }
    }
}
