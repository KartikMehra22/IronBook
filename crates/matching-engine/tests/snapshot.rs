#![allow(clippy::pedantic, clippy::expect_used, clippy::unwrap_used)]

//! Snapshot / restore round-trip and determinism (spec §5.2.5).

use matching_engine::*;

fn order(seq: u64, side: Side, qty: u64, price: i64, tif: TimeInForce) -> Order {
    Order {
        platform_seq: seq,
        platform_ts_ns: seq,
        client_order_id: ClientOrderId::new(1, seq),
        session_token: SessionToken([0; 32]),
        side,
        qty: Qty(qty),
        kind: OrderType::Limit {
            price: Price(price),
            tif,
        },
    }
}

#[test]
fn empty_book_roundtrips() {
    let a = OrderBook::new();
    let bytes = a.snapshot();
    let b = OrderBook::restore(&bytes).expect("restore");
    assert_eq!(b.best_bid(), None);
    assert_eq!(b.best_ask(), None);
    assert_eq!(b.order_count(), 0);
}

#[test]
fn snapshot_roundtrip_preserves_match_outcome() {
    // Build a non-trivial book.
    let mut a = OrderBook::new();
    let orders: Vec<Order> = (1u64..=20)
        .map(|i| {
            order(
                i,
                if i % 2 == 0 { Side::Bid } else { Side::Ask },
                10,
                100 + (i as i64 % 5),
                TimeInForce::Gtc,
            )
        })
        .collect();
    for o in &orders {
        let _ = a.match_order(o.clone());
    }

    // Serialize → deserialize.
    let bytes = a.snapshot();
    let mut b = OrderBook::restore(&bytes).expect("restore");

    // A new probe must produce the same fills against a and b.
    let probe = order(100, Side::Bid, 5, 110, TimeInForce::Ioc);
    let fills_a = a.match_order(probe.clone());
    let fills_b = b.match_order(probe);
    assert_eq!(fills_a, fills_b);
}

#[test]
fn snapshot_bytes_are_deterministic() {
    let mut a = OrderBook::new();
    let mut b = OrderBook::new();
    let inputs: Vec<Order> = (1u64..=15)
        .map(|i| {
            order(
                i,
                if i % 2 == 0 { Side::Bid } else { Side::Ask },
                10,
                100 + (i as i64 % 5),
                TimeInForce::Gtc,
            )
        })
        .collect();
    for o in &inputs {
        let _ = a.match_order(o.clone());
        let _ = b.match_order(o.clone());
    }
    assert_eq!(a.snapshot(), b.snapshot());
}

#[test]
fn cancel_after_restore_works() {
    let mut a = OrderBook::new();
    a.rest(order(1, Side::Bid, 10, 100, TimeInForce::Gtc));
    let bytes = a.snapshot();
    let mut b = OrderBook::restore(&bytes).expect("restore");
    assert!(b.cancel(ClientOrderId::new(1, 1)));
    assert_eq!(b.best_bid(), None);
}
