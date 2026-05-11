//! `IronBook` matching engine — order book + price-time priority matcher.
//!
//! Phase 2 baseline: integer prices + quantities (no floats), `BTreeMap`-of-
//! price-levels order book, GTC/IOC/FOK + market semantics, deterministic
//! snapshot/restore.
//!
//! Spec references:
//! - §3.5 reference oracle deep-dive
//! - §5.1 correctness invariants
//! - §5.2 internal data model

#![doc(html_no_source)]
// Pedantic exceptions for this crate:
//   - cast_possible_truncation: u128 → u64 in ClientOrderId/snapshot is intentional.
//   - needless_pass_by_value: Resting/RestingSnap are small and ownership-transferring is clear.
//   - expect_used / unwrap_used in tests + matcher unreachables (post-check) is intentional.
//   - doc_markdown is over-eager on plain English mentions of types.
//   - missing_panics_doc / missing_errors_doc tracked inline; not the formal doc style.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::needless_pass_by_value,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::single_match_else
)]

pub mod book;
pub mod match_engine;
pub mod snapshot;
pub mod types;

pub use book::OrderBook;
pub use types::{
    ClientOrderId, Fill, Order, OrderType, Price, Qty, SessionToken, Side, TimeInForce,
};

/// Crate version marker — useful smoke check.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
