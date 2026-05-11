//! Order book: `BTreeMap<Price, VecDeque<Resting>>` per side.
//!
//! Best bid is the *highest* price with at least one resting order; best ask
//! is the *lowest*. `BTreeMap` gives O(log N) access to either, plus
//! ordered iteration which the matcher walks.
//!
//! Per-level `VecDeque` enforces price-time priority within a level: orders
//! arrive in `platform_seq` order, push_back; matching pops_front. O(1) at
//! both ends.

use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::types::{ClientOrderId, Order, OrderType, Price, Qty, SessionToken, Side};

/// An order currently resting on the book.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resting {
    pub platform_seq: u64,
    pub platform_ts_ns: u64,
    pub client_order_id: ClientOrderId,
    pub session_token: SessionToken,
    pub qty_remaining: Qty,
}

/// The order book. Two BTreeMaps, one index, one monotonic trade-id counter.
#[derive(Default, Clone, Debug)]
pub struct OrderBook {
    pub(crate) bids: BTreeMap<Price, VecDeque<Resting>>,
    pub(crate) asks: BTreeMap<Price, VecDeque<Resting>>,
    pub(crate) by_id: HashMap<ClientOrderId, (Side, Price)>,
    pub(crate) next_trade_id: u64,
}

impl OrderBook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Highest bid price with at least one resting order, or None.
    #[must_use]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.iter().next_back().map(|(p, _)| *p)
    }

    /// Lowest ask price with at least one resting order, or None.
    #[must_use]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.iter().next().map(|(p, _)| *p)
    }

    /// Append a resting order to its price level (price-time priority by
    /// arrival order). Market orders never rest — they're discarded here.
    pub fn rest(&mut self, o: Order) {
        // Market orders never rest — guard with `let ... else`.
        let OrderType::Limit { price, .. } = o.kind else {
            return;
        };
        let side = o.side;
        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let queue = book.entry(price).or_default();
        queue.push_back(Resting {
            platform_seq: o.platform_seq,
            platform_ts_ns: o.platform_ts_ns,
            client_order_id: o.client_order_id,
            session_token: o.session_token,
            qty_remaining: o.qty,
        });
        self.by_id.insert(o.client_order_id, (side, price));
    }

    /// Remove a resting order by client order id. Returns true if found.
    pub fn cancel(&mut self, id: ClientOrderId) -> bool {
        let Some((side, price)) = self.by_id.remove(&id) else {
            return false;
        };
        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let Some(queue) = book.get_mut(&price) else {
            return false;
        };
        let Some(idx) = queue.iter().position(|r| r.client_order_id == id) else {
            return false;
        };
        queue.remove(idx);
        if queue.is_empty() {
            book.remove(&price);
        }
        true
    }

    /// Number of resting orders. O(N) — useful for tests, don't call in hot path.
    #[must_use]
    pub fn order_count(&self) -> usize {
        let bids: usize = self.bids.values().map(VecDeque::len).sum();
        let asks: usize = self.asks.values().map(VecDeque::len).sum();
        bids + asks
    }

    // -- crate-internal hooks for the matcher --------------------------------

    pub(crate) fn next_trade_id(&mut self) -> u64 {
        let id = self.next_trade_id;
        self.next_trade_id += 1;
        id
    }

    /// Yields the price + queue of the best level facing `taker_side`. Bid
    /// takers match against the lowest ask; ask takers match against the
    /// highest bid.
    pub(crate) fn best_for(&mut self, taker_side: Side) -> Option<(Price, &mut VecDeque<Resting>)> {
        match taker_side {
            Side::Bid => self.asks.iter_mut().next().map(|(p, q)| (*p, q)),
            Side::Ask => self.bids.iter_mut().next_back().map(|(p, q)| (*p, q)),
        }
    }

    pub(crate) fn drop_level_if_empty(&mut self, taker_side: Side, price: Price) {
        // `taker_side = Bid` means we just consumed from the asks; symmetric.
        let book = match taker_side {
            Side::Bid => &mut self.asks,
            Side::Ask => &mut self.bids,
        };
        if book.get(&price).is_some_and(VecDeque::is_empty) {
            book.remove(&price);
        }
    }

    pub(crate) fn forget_id(&mut self, id: ClientOrderId) {
        self.by_id.remove(&id);
    }
}
