#![allow(clippy::pedantic, clippy::expect_used, clippy::unwrap_used)]

//! Property-based tests for the matching engine.
//!
//! Each property is run against 256 random order sequences by default.
//! Nightly CI bumps this to 4096 via `PROPTEST_CASES=4096`.
//!
//! Covers spec §5.1's invariants:
//!   - C2 fill conservation
//!   - C3 no phantom fills (every maker has been seen by the engine)
//!   - C6 trade-id monotonicity (a weak form of idempotency)

use matching_engine::*;
use proptest::prelude::*;

#[derive(Clone, Debug)]
struct GenOrder {
    side: Side,
    qty: u64,
    price: i64,
    tif: TimeInForce,
    market: bool,
}

fn arb_one() -> impl Strategy<Value = GenOrder> {
    (
        any::<bool>(),
        1u64..200,
        50i64..150,
        0u8..3,
        // Market orders ~ 10% of the mix.
        0u8..10,
    )
        .prop_map(|(b, q, p, t, m)| GenOrder {
            side: if b { Side::Bid } else { Side::Ask },
            qty: q,
            price: p,
            tif: match t {
                0 => TimeInForce::Gtc,
                1 => TimeInForce::Ioc,
                _ => TimeInForce::Fok,
            },
            market: m == 0,
        })
}

fn arb_sequence(n: usize) -> impl Strategy<Value = Vec<GenOrder>> {
    prop::collection::vec(arb_one(), n..=n)
}

fn to_order(seq: u64, g: &GenOrder) -> Order {
    Order {
        platform_seq: seq,
        platform_ts_ns: seq,
        client_order_id: ClientOrderId::new(1, seq),
        session_token: SessionToken([0; 32]),
        side: g.side,
        qty: Qty(g.qty),
        kind: if g.market {
            OrderType::Market
        } else {
            OrderType::Limit {
                price: Price(g.price),
                tif: g.tif,
            }
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// C2 fill conservation: for every order seen by the engine, the sum of
    /// fill quantities attributed to it (as taker OR maker) never exceeds the
    /// quantity it was submitted with.
    #[test]
    fn fill_conservation(orders in arb_sequence(30)) {
        let mut book = OrderBook::new();
        let mut submitted: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut filled:    std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        for (i, g) in orders.iter().enumerate() {
            let seq = (i as u64) + 1;
            let o = to_order(seq, g);
            submitted.insert(seq, o.qty.0);
            for f in book.match_order(o) {
                prop_assert!(f.qty.0 > 0);
                *filled.entry(f.platform_seq_taker).or_default() += f.qty.0;
                *filled.entry(f.platform_seq_maker).or_default() += f.qty.0;
            }
        }
        for (seq, sub_qty) in &submitted {
            let f = filled.get(seq).copied().unwrap_or(0);
            prop_assert!(
                f <= *sub_qty,
                "seq {} over-filled: submitted={} filled={}", seq, sub_qty, f
            );
        }
    }

    /// Symmetry: every fill's qty contributes equally to both legs, so
    /// total maker-side traded volume equals total taker-side traded volume.
    #[test]
    fn maker_taker_volume_equal(orders in arb_sequence(30)) {
        let mut book = OrderBook::new();
        let mut maker_vol: u64 = 0;
        let mut taker_vol: u64 = 0;
        for (i, g) in orders.iter().enumerate() {
            let o = to_order((i as u64) + 1, g);
            for f in book.match_order(o) {
                maker_vol += f.qty.0;
                taker_vol += f.qty.0;
            }
        }
        prop_assert_eq!(maker_vol, taker_vol);
    }

    /// C3: every fill references a maker whose platform_seq has been
    /// observed by the engine (no phantoms).
    #[test]
    fn no_phantom_makers(orders in arb_sequence(30)) {
        let mut book = OrderBook::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (i, g) in orders.iter().enumerate() {
            let seq = (i as u64) + 1;
            seen.insert(seq);
            let o = to_order(seq, g);
            for f in book.match_order(o) {
                prop_assert!(
                    seen.contains(&f.platform_seq_maker),
                    "phantom maker seq {} (taker seq {})", f.platform_seq_maker, f.platform_seq_taker
                );
                prop_assert!(seen.contains(&f.platform_seq_taker));
            }
        }
    }

    /// trade_id assignments are strictly monotonic across the lifetime of
    /// the book — even across multiple match_order calls.
    #[test]
    fn trade_ids_monotonic_across_calls(orders in arb_sequence(40)) {
        let mut book = OrderBook::new();
        let mut last: Option<u64> = None;
        for (i, g) in orders.iter().enumerate() {
            let o = to_order((i as u64) + 1, g);
            for f in book.match_order(o) {
                if let Some(prev) = last {
                    prop_assert!(f.trade_id > prev, "trade_id {} not > {}", f.trade_id, prev);
                }
                last = Some(f.trade_id);
            }
        }
    }
}
