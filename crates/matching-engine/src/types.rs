//! Primitive types for the order book and matcher.
//!
//! All prices are integer ticks; all quantities are unsigned. No floats.

use serde::{Deserialize, Serialize};

/// Price in ticks. `i64` lets us model crosses around zero without loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Price(pub i64);

/// Quantity. Unsigned; subtraction is checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Qty(pub u64);

impl Qty {
    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub fn checked_sub(self, rhs: Qty) -> Option<Qty> {
        self.0.checked_sub(rhs.0).map(Qty)
    }

    #[must_use]
    pub fn min(self, rhs: Qty) -> Qty {
        Qty(self.0.min(rhs.0))
    }
}

/// Which side of the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Bid,
    Ask,
}

/// Time-in-force semantics for a taker order's remainder.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good-til-cancelled — remainder rests on the book.
    Gtc,
    /// Immediate-or-cancel — remainder is discarded.
    Ioc,
    /// Fill-or-kill — all-or-nothing; partial → reject + roll back fills.
    Fok,
}

/// Order kind: limit (with price + TIF) or market (no price, no TIF).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit { price: Price, tif: TimeInForce },
    Market,
}

/// Client order id, packed as `(bot_id, local_seq)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub u128);

impl ClientOrderId {
    #[must_use]
    pub const fn new(bot_id: u64, local_seq: u64) -> Self {
        Self(((bot_id as u128) << 64) | (local_seq as u128))
    }

    #[must_use]
    pub const fn bot_id(self) -> u64 {
        (self.0 >> 64) as u64
    }

    #[must_use]
    pub const fn local_seq(self) -> u64 {
        (self.0 & 0xFFFF_FFFF_FFFF_FFFF) as u64
    }
}

/// 32 bytes of opaque per-run identity token. The matching engine never
/// interprets it — the gateway sets it; the oracle echoes it back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionToken(pub [u8; 32]);

/// A normalized, gateway-stamped order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Order {
    pub platform_seq: u64,
    pub platform_ts_ns: u64,
    pub client_order_id: ClientOrderId,
    pub session_token: SessionToken,
    pub side: Side,
    pub qty: Qty,
    pub kind: OrderType,
}

impl Order {
    /// Helper: clone of this order with a different quantity. Used by the
    /// matcher when resting the remainder of a partial fill.
    #[must_use]
    pub fn with_qty(&self, new_qty: Qty) -> Self {
        let mut o = self.clone();
        o.qty = new_qty;
        o
    }
}

/// A fill produced by the matcher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub trade_id: u64,
    pub platform_seq_taker: u64,
    pub platform_seq_maker: u64,
    pub price: Price,
    pub qty: Qty,
    pub ts_ns: u64,
}
