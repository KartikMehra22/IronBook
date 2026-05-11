//! Time-service core (no transport): a monotonic stamp issuer.
//!
//! Each call to [`Clock::reserve`] hands out a contiguous block of
//! (sequence, timestamp) stamps. The sequence is strictly monotonic across
//! the process lifetime; the timestamp is monotonic nanoseconds since the
//! Clock was constructed plus a configurable boot offset.
//!
//! Persistence (high-water-mark on disk) is the responsibility of the
//! binary in `src/main.rs`; the library only manages the in-process counter.

#![allow(clippy::module_name_repetitions, clippy::missing_panics_doc)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Monotonic stamp generator.
pub struct Clock {
    next_seq: AtomicU64,
    boot_ts_ns: u64,
    start_instant: Instant,
}

impl Clock {
    /// Construct a clock that begins issuing sequence numbers at `starting_seq`.
    /// The first timestamp returned is `SystemTime::now()`; subsequent
    /// timestamps add the monotonic elapsed since this constructor.
    #[must_use]
    pub fn new(starting_seq: u64) -> Self {
        let boot_ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        Self {
            next_seq: AtomicU64::new(starting_seq),
            boot_ts_ns,
            start_instant: Instant::now(),
        }
    }

    /// Reserve `n` stamps and return the first `(seq, ts_ns)`. Thread-safe.
    pub fn reserve(&self, n: u64) -> (u64, u64) {
        let seq = self.next_seq.fetch_add(n, Ordering::SeqCst);
        let elapsed = u64::try_from(self.start_instant.elapsed().as_nanos()).unwrap_or(u64::MAX);
        (seq, self.boot_ts_ns.saturating_add(elapsed))
    }

    /// Highest sequence number reserved so far + 1. Used for persistence.
    pub fn high_watermark(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_monotonic_across_reservations() {
        let c = Clock::new(0);
        let (s1, _) = c.reserve(10);
        let (s2, _) = c.reserve(5);
        assert_eq!(s1, 0);
        assert_eq!(s2, 10);
        assert_eq!(c.high_watermark(), 15);
    }

    #[test]
    fn timestamps_are_monotonic() {
        let c = Clock::new(0);
        let (_, t1) = c.reserve(1);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let (_, t2) = c.reserve(1);
        assert!(t2 > t1, "expected t2 > t1, got {t1} >= {t2}");
    }

    #[test]
    fn restart_with_higher_starting_seq_continues_monotonically() {
        let c1 = Clock::new(0);
        let (s, _) = c1.reserve(100);
        assert_eq!(s, 0);
        let hwm = c1.high_watermark();
        // simulate restart with a safety gap of 10k
        let c2 = Clock::new(hwm + 10_000);
        let (s2, _) = c2.reserve(1);
        assert!(s2 >= hwm + 10_000);
    }
}
