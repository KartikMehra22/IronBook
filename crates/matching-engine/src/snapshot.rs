//! Deterministic snapshot/restore for crash recovery (spec §5.2.5).
//!
//! Bincode serialization of the order book — same input sequence + same
//! snapshot bytes = identical state on restore.

use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use serde::{Deserialize, Serialize};

use crate::book::{OrderBook, Resting};
use crate::types::{ClientOrderId, Price, Qty, SessionToken, Side};

#[derive(Serialize, Deserialize)]
struct Snap {
    bids: Vec<(i64, Vec<RestingSnap>)>,
    asks: Vec<(i64, Vec<RestingSnap>)>,
    next_trade_id: u64,
}

#[derive(Serialize, Deserialize)]
struct RestingSnap {
    platform_seq: u64,
    platform_ts_ns: u64,
    client_order_id_hi: u64,
    client_order_id_lo: u64,
    session_token: [u8; 32],
    qty_remaining: u64,
}

impl OrderBook {
    /// Serialize to deterministic bytes (bincode v2).
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        let snap = Snap {
            bids: self
                .bids
                .iter()
                .map(|(p, q)| (p.0, q.iter().map(snap_one).collect()))
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(p, q)| (p.0, q.iter().map(snap_one).collect()))
                .collect(),
            next_trade_id: self.next_trade_id,
        };
        encode_to_vec(&snap, config::standard()).expect("infallible bincode encode")
    }

    /// Restore a book from snapshot bytes. Order of resting orders within
    /// each price level is preserved.
    ///
    /// # Errors
    /// Returns a bincode `DecodeError` if the bytes don't decode to a valid `Snap`.
    pub fn restore(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (snap, _): (Snap, _) = decode_from_slice(bytes, config::standard())?;
        let mut b = OrderBook::new();
        for (p, q) in snap.bids {
            let price = Price(p);
            for r in q {
                let r = restore_one(r);
                b.bids.entry(price).or_default().push_back(r.clone());
                b.by_id.insert(r.client_order_id, (Side::Bid, price));
            }
        }
        for (p, q) in snap.asks {
            let price = Price(p);
            for r in q {
                let r = restore_one(r);
                b.asks.entry(price).or_default().push_back(r.clone());
                b.by_id.insert(r.client_order_id, (Side::Ask, price));
            }
        }
        b.next_trade_id = snap.next_trade_id;
        Ok(b)
    }
}

fn snap_one(r: &Resting) -> RestingSnap {
    let id = r.client_order_id.0;
    RestingSnap {
        platform_seq: r.platform_seq,
        platform_ts_ns: r.platform_ts_ns,
        client_order_id_hi: (id >> 64) as u64,
        client_order_id_lo: id as u64,
        session_token: r.session_token.0,
        qty_remaining: r.qty_remaining.0,
    }
}

fn restore_one(s: RestingSnap) -> Resting {
    Resting {
        platform_seq: s.platform_seq,
        platform_ts_ns: s.platform_ts_ns,
        client_order_id: ClientOrderId(
            (u128::from(s.client_order_id_hi) << 64) | u128::from(s.client_order_id_lo),
        ),
        session_token: SessionToken(s.session_token),
        qty_remaining: Qty(s.qty_remaining),
    }
}
