//! Price-time priority matcher.

use crate::book::OrderBook;
use crate::types::{Fill, Order, OrderType, Price, Side, TimeInForce};

impl OrderBook {
    /// Match a taker order against the book.
    ///
    /// Walks the best opposite price level, consumes resting orders FIFO,
    /// then advances to the next price level until: (a) the taker is fully
    /// filled, (b) the best opposite no longer crosses the taker's limit
    /// price, (c) FOK pre-flight rejection.
    ///
    /// Per TIF semantics, any remainder either rests (GTC) or is discarded
    /// (IOC, Market). FOK's all-or-nothing check happens before any state is
    /// mutated (no rollback needed).
    pub fn match_order(&mut self, taker: Order) -> Vec<Fill> {
        // FOK pre-flight: can we fill in full at acceptable prices? If not,
        // reject without touching the book.
        if let OrderType::Limit {
            price: lim,
            tif: TimeInForce::Fok,
        } = taker.kind
        {
            if !self.can_fill_full(taker.side, taker.qty, lim) {
                return Vec::new();
            }
        }

        let mut fills = Vec::new();
        let mut remaining = taker.qty;

        while remaining.0 > 0 {
            let Some((price, queue)) = self.best_for(taker.side) else {
                break;
            };
            if !crosses(taker.kind, taker.side, price) {
                break;
            }

            // Drain this price level until empty or remaining hits zero.
            let mut emptied_ids = Vec::new();
            while remaining.0 > 0 {
                let Some(front) = queue.front_mut() else {
                    break;
                };
                let traded = remaining.min(front.qty_remaining);
                fills.push(Fill {
                    trade_id: 0, // patched after the loop (avoids re-borrow)
                    platform_seq_taker: taker.platform_seq,
                    platform_seq_maker: front.platform_seq,
                    price,
                    qty: traded,
                    ts_ns: taker.platform_ts_ns,
                });
                remaining = remaining
                    .checked_sub(traded)
                    .expect("traded <= remaining by min()");
                front.qty_remaining = front
                    .qty_remaining
                    .checked_sub(traded)
                    .expect("traded <= qty_remaining by min()");
                if front.qty_remaining.0 == 0 {
                    emptied_ids.push(front.client_order_id);
                    queue.pop_front();
                }
            }

            // Done mutating `queue` for this level. Now safe to update `by_id`.
            for id in emptied_ids {
                self.forget_id(id);
            }
            self.drop_level_if_empty(taker.side, price);
        }

        // Assign monotonic trade ids to the fills produced.
        for f in &mut fills {
            f.trade_id = self.next_trade_id();
        }

        // Rest the remainder according to TIF.
        if remaining.0 > 0 {
            match taker.kind {
                OrderType::Limit {
                    tif: TimeInForce::Gtc,
                    ..
                } => {
                    self.rest(taker.with_qty(remaining));
                }
                OrderType::Limit {
                    tif: TimeInForce::Ioc | TimeInForce::Fok,
                    ..
                }
                | OrderType::Market => {
                    // Discard. FOK is unreachable here because pre-flight
                    // returns early; included for exhaustiveness.
                }
            }
        }

        fills
    }

    /// FOK pre-flight: would a sweep starting at `lim` fill `qty` units?
    fn can_fill_full(&self, taker_side: Side, qty: crate::types::Qty, lim: Price) -> bool {
        let mut remaining: u64 = qty.0;
        let levels: Box<
            dyn Iterator<Item = (&Price, &std::collections::VecDeque<crate::book::Resting>)>,
        > = match taker_side {
            Side::Bid => Box::new(self.asks.iter()),
            Side::Ask => Box::new(self.bids.iter().rev()),
        };
        for (p, q) in levels {
            if !crosses_price(taker_side, *p, lim) {
                break;
            }
            for r in q {
                remaining = remaining.saturating_sub(r.qty_remaining.0);
                if remaining == 0 {
                    return true;
                }
            }
        }
        false
    }
}

/// Does a taker order's kind/side cross the given resting price level?
fn crosses(kind: OrderType, taker_side: Side, level: Price) -> bool {
    match kind {
        OrderType::Limit { price, .. } => crosses_price(taker_side, level, price),
        OrderType::Market => true,
    }
}

fn crosses_price(taker_side: Side, level: Price, taker_price: Price) -> bool {
    match taker_side {
        Side::Bid => level <= taker_price,
        Side::Ask => level >= taker_price,
    }
}
