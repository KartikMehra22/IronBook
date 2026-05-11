#![allow(clippy::pedantic, clippy::expect_used, clippy::unwrap_used)]

//! Hand-written matcher cases covering the core invariants. Property tests
//! live in `proptest.rs`.

use matching_engine::*;

fn order(seq: u64, side: Side, qty: u64, price: i64, tif: TimeInForce) -> Order {
    Order {
        platform_seq: seq,
        platform_ts_ns: seq, // tests use seq as ts for determinism
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

fn market(seq: u64, side: Side, qty: u64) -> Order {
    Order {
        platform_seq: seq,
        platform_ts_ns: seq,
        client_order_id: ClientOrderId::new(2, seq),
        session_token: SessionToken([0; 32]),
        side,
        qty: Qty(qty),
        kind: OrderType::Market,
    }
}

#[test]
fn empty_book_has_no_best() {
    let b = OrderBook::new();
    assert!(b.best_bid().is_none());
    assert!(b.best_ask().is_none());
}

#[test]
fn resting_a_bid_at_100_advertises_it_as_best_bid() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Bid, 10, 100, TimeInForce::Gtc));
    assert_eq!(b.best_bid(), Some(Price(100)));
    assert_eq!(b.best_ask(), None);
}

#[test]
fn taker_crosses_resting_ask_one_fill() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 10, 100, TimeInForce::Gtc));
    let fills = b.match_order(order(2, Side::Bid, 5, 100, TimeInForce::Ioc));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, Qty(5));
    assert_eq!(fills[0].price, Price(100));
    // Remaining 5 still rests at 100.
    assert_eq!(b.best_ask(), Some(Price(100)));
}

#[test]
fn fok_rejects_partial_fill_atomically() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 5, 100, TimeInForce::Gtc));
    let fills = b.match_order(order(2, Side::Bid, 10, 100, TimeInForce::Fok));
    assert!(fills.is_empty(), "FOK rejected → no fills");
    assert_eq!(b.best_ask(), Some(Price(100)));
}

#[test]
fn fok_fills_when_full_size_is_available() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 10, 100, TimeInForce::Gtc));
    let fills = b.match_order(order(2, Side::Bid, 10, 100, TimeInForce::Fok));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, Qty(10));
    assert_eq!(b.best_ask(), None);
}

#[test]
fn ioc_partial_discards_remainder() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 5, 100, TimeInForce::Gtc));
    let fills = b.match_order(order(2, Side::Bid, 10, 100, TimeInForce::Ioc));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, Qty(5));
    assert!(b.best_bid().is_none(), "IOC remainder is not rested");
}

#[test]
fn ptp_walks_levels_in_order() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 3, 100, TimeInForce::Gtc));
    b.rest(order(2, Side::Ask, 3, 101, TimeInForce::Gtc));
    let fills = b.match_order(order(3, Side::Bid, 5, 102, TimeInForce::Ioc));
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].price, Price(100));
    assert_eq!(fills[0].qty, Qty(3));
    assert_eq!(fills[1].price, Price(101));
    assert_eq!(fills[1].qty, Qty(2));
    assert_eq!(b.best_ask(), Some(Price(101)));
}

#[test]
fn time_priority_within_level() {
    let mut b = OrderBook::new();
    // Two asks at the same price; seq=1 arrives first, must fill first.
    b.rest(order(1, Side::Ask, 5, 100, TimeInForce::Gtc));
    b.rest(order(2, Side::Ask, 5, 100, TimeInForce::Gtc));
    let fills = b.match_order(order(3, Side::Bid, 5, 100, TimeInForce::Ioc));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].platform_seq_maker, 1);
    assert_eq!(fills[0].qty, Qty(5));
}

#[test]
fn market_order_walks_book() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 3, 100, TimeInForce::Gtc));
    b.rest(order(2, Side::Ask, 3, 200, TimeInForce::Gtc));
    let fills = b.match_order(market(3, Side::Bid, 5));
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].price, Price(100));
    assert_eq!(fills[1].price, Price(200));
}

#[test]
fn cancel_removes_resting_order() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Bid, 10, 100, TimeInForce::Gtc));
    assert!(b.cancel(ClientOrderId::new(1, 1)));
    assert_eq!(b.best_bid(), None);
    // Second cancel is a no-op.
    assert!(!b.cancel(ClientOrderId::new(1, 1)));
}

#[test]
fn trade_ids_are_monotonic() {
    let mut b = OrderBook::new();
    b.rest(order(1, Side::Ask, 3, 100, TimeInForce::Gtc));
    b.rest(order(2, Side::Ask, 3, 101, TimeInForce::Gtc));
    let fills = b.match_order(order(3, Side::Bid, 6, 110, TimeInForce::Ioc));
    assert_eq!(fills.len(), 2);
    assert!(fills[0].trade_id < fills[1].trade_id);
}
