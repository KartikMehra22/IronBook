#![allow(clippy::pedantic, clippy::expect_used, clippy::unwrap_used)]

//! Criterion benches for the matching engine.
//!
//! Targets (spec §7.7):
//!   - match_limit_uncrossed (resting → no fill)  ≥ 1.5M ops/s
//!   - match_limit_one_fill                       ≥ 800k ops/s
//!   - match_limit_walk_5_levels                  ≥ 200k ops/s

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use matching_engine::{
    ClientOrderId, Order, OrderBook, OrderType, Price, Qty, SessionToken, Side, TimeInForce,
};

fn make_order(seq: u64, side: Side, qty: u64, price: i64, tif: TimeInForce) -> Order {
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

fn make_book(n_per_side: u64) -> OrderBook {
    let mut b = OrderBook::new();
    for i in 0..n_per_side {
        b.rest(make_order(
            i * 2,
            Side::Ask,
            10,
            100 + (i as i64),
            TimeInForce::Gtc,
        ));
        b.rest(make_order(
            i * 2 + 1,
            Side::Bid,
            10,
            99 - (i as i64),
            TimeInForce::Gtc,
        ));
    }
    b
}

// `match_limit_uncrossed` never mutates state (price doesn't cross + Ioc),
// so we can iter() against a single book and measure pure matcher cost.
fn bench_uncrossed(c: &mut Criterion) {
    let mut book = make_book(1_000);
    c.bench_function("match_limit_uncrossed", |b| {
        b.iter(|| {
            let _ = black_box(book.match_order(make_order(
                1_000_000,
                Side::Bid,
                5,
                50,
                TimeInForce::Ioc,
            )));
        });
    });
}

// `match_limit_one_fill` and `walk_5_levels` consume state, so iter_batched
// with a small (100×2) book keeps the per-iteration clone cheap.
fn bench_one_fill(c: &mut Criterion) {
    let template = make_book(100);
    c.bench_function("match_limit_one_fill", |b| {
        b.iter_batched(
            || template.clone(),
            |mut book| {
                let _ = black_box(book.match_order(make_order(
                    1_000_000,
                    Side::Bid,
                    5,
                    100,
                    TimeInForce::Ioc,
                )));
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_walk_5(c: &mut Criterion) {
    let template = make_book(100);
    c.bench_function("match_limit_walk_5_levels", |b| {
        b.iter_batched(
            || template.clone(),
            |mut book| {
                let _ = black_box(book.match_order(make_order(
                    1_000_000,
                    Side::Bid,
                    45,
                    200,
                    TimeInForce::Ioc,
                )));
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_uncrossed, bench_one_fill, bench_walk_5);
criterion_main!(benches);
