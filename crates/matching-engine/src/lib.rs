//! IronBook matching engine — order book + matcher.
//! Phase 1 stub. Real implementation lands in Phase 2 Task 2.5.x.

#![doc(html_no_source)]

/// Phase 1 marker — proves the crate compiles.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn version_string_set() {
        assert!(!version().is_empty());
    }
}
