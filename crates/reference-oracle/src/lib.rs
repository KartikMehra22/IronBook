//! Reference-oracle library: tonic `OrderIntake` server wrapping
//! the [`matching_engine::OrderBook`].
//!
//! Each call to `Submit` flows through the engine inside a `parking_lot::Mutex`.
//! Phase 4 swaps the Mutex for a single-threaded actor + crossbeam channel
//! once a profiler proves it matters; for hackathon scope, the Mutex is
//! cheaper than the actor's channel overhead and is exposed honestly in
//! the latency budget (spec §2.3).

#![allow(
    clippy::module_name_repetitions,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::match_same_arms
)]

use std::time::{SystemTime, UNIX_EPOCH};

use ironbook_proto::gen::ironbook::v1::{
    order_intake_server::OrderIntake, Ack, Fill as WireFill, NormalizedOrder, OrderType as WireOT,
    Reply, Side as WireSide, TimeInForce as WireTif,
};
use matching_engine::{
    ClientOrderId, OrderBook, OrderType, Price, Qty, SessionToken, Side, TimeInForce,
};
use parking_lot::Mutex;
use tonic::{Request, Response, Status};

/// gRPC service struct holding a single OrderBook protected by a Mutex.
pub struct Oracle {
    pub book: Mutex<OrderBook>,
}

impl Default for Oracle {
    fn default() -> Self {
        Self {
            book: Mutex::new(OrderBook::new()),
        }
    }
}

impl Oracle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[tonic::async_trait]
impl OrderIntake for Oracle {
    async fn submit(&self, req: Request<NormalizedOrder>) -> Result<Response<Reply>, Status> {
        let n = req.into_inner();
        let order = match wire_to_order(&n) {
            Ok(o) => o,
            Err(reason) => {
                return Ok(Response::new(Reply {
                    ack: Some(Ack {
                        platform_seq: n.platform_seq,
                        ack_ts_ns: now_ns(),
                        status: 1,
                        message: reason,
                    }),
                    fills: Vec::new(),
                }));
            }
        };

        let fills = self.book.lock().match_order(order.clone());
        let wire_fills: Vec<WireFill> = fills
            .into_iter()
            .map(|f| WireFill {
                trade_id: f.trade_id,
                platform_seq_taker: f.platform_seq_taker,
                platform_seq_maker: f.platform_seq_maker,
                price: f.price.0,
                qty: f.qty.0,
                ts_ns: f.ts_ns,
            })
            .collect();

        Ok(Response::new(Reply {
            ack: Some(Ack {
                platform_seq: order.platform_seq,
                ack_ts_ns: now_ns(),
                status: 0,
                message: String::new(),
            }),
            fills: wire_fills,
        }))
    }
}

/// Translate a wire `NormalizedOrder` into the matching engine's `Order` type.
/// Returns Err with a human-readable reason on invalid enum values.
fn wire_to_order(n: &NormalizedOrder) -> Result<matching_engine::Order, String> {
    let side = match WireSide::try_from(n.side).map_err(|_| "bad side")? {
        WireSide::Bid => Side::Bid,
        WireSide::Ask => Side::Ask,
        WireSide::Unspecified => return Err("unspecified side".into()),
    };
    let tif = match WireTif::try_from(n.tif).map_err(|_| "bad tif")? {
        WireTif::Gtc => TimeInForce::Gtc,
        WireTif::Ioc => TimeInForce::Ioc,
        WireTif::Fok => TimeInForce::Fok,
        WireTif::Unspecified => TimeInForce::Gtc, // default
    };
    let kind = match WireOT::try_from(n.order_type).map_err(|_| "bad order type")? {
        WireOT::Limit => OrderType::Limit {
            price: Price(n.price),
            tif,
        },
        WireOT::Market => OrderType::Market,
        WireOT::Unspecified => return Err("unspecified order type".into()),
    };

    let coid_bytes: [u8; 16] = n
        .client_order_id
        .as_slice()
        .try_into()
        .map_err(|_| "client_order_id must be 16 bytes")?;
    let token_bytes: [u8; 32] = n
        .session_token
        .as_slice()
        .try_into()
        .map_err(|_| "session_token must be 32 bytes")?;

    Ok(matching_engine::Order {
        platform_seq: n.platform_seq,
        platform_ts_ns: n.platform_ts_ns,
        client_order_id: ClientOrderId(u128::from_be_bytes(coid_bytes)),
        session_token: SessionToken(token_bytes),
        side,
        qty: Qty(n.qty),
        kind,
    })
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}
