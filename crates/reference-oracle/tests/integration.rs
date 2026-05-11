//! End-to-end gRPC test for reference-oracle: spin the tonic server in a
//! background tokio task, hit it with a real OrderIntake client, and assert
//! the wire response shape matches what the matching engine produces.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::pedantic)]

use std::net::SocketAddr;
use std::time::Duration;

use ironbook_proto::gen::ironbook::v1::{
    order_intake_client::OrderIntakeClient, order_intake_server::OrderIntakeServer,
    NormalizedOrder, OrderType as WireOT, Side as WireSide, TimeInForce as WireTif,
};
use reference_oracle::Oracle;
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Spawn the oracle on an OS-chosen port and return the bound address.
async fn spawn_oracle() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(OrderIntakeServer::new(Oracle::new()))
            .serve_with_incoming(incoming)
            .await;
    });

    // Give the server a few ms to start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

fn order(seq: u64, side: WireSide, qty: u64, price: i64, tif: WireTif) -> NormalizedOrder {
    NormalizedOrder {
        platform_seq: seq,
        platform_ts_ns: seq,
        client_order_id: vec![0u8; 16],
        session_token: vec![0u8; 32],
        side: i32::from(side),
        qty,
        price,
        order_type: i32::from(WireOT::Limit),
        tif: i32::from(tif),
    }
}

#[tokio::test]
async fn submit_buy_then_sell_results_in_fill() {
    let addr = spawn_oracle().await;
    let mut c = OrderIntakeClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    // Resting bid at 100 qty 10.
    let r1 = c
        .submit(order(1, WireSide::Bid, 10, 100, WireTif::Gtc))
        .await
        .expect("submit 1")
        .into_inner();
    assert_eq!(r1.ack.as_ref().expect("ack").status, 0);
    assert!(r1.fills.is_empty(), "first order rests");

    // Crossing ask at 100 qty 5 — should produce one fill at 100/5.
    let r2 = c
        .submit(order(2, WireSide::Ask, 5, 100, WireTif::Ioc))
        .await
        .expect("submit 2")
        .into_inner();
    assert_eq!(r2.ack.as_ref().expect("ack").status, 0);
    assert_eq!(r2.fills.len(), 1);
    let f = &r2.fills[0];
    assert_eq!(f.qty, 5);
    assert_eq!(f.price, 100);
    assert_eq!(f.platform_seq_taker, 2);
    assert_eq!(f.platform_seq_maker, 1);
}

#[tokio::test]
async fn malformed_order_returns_nonzero_ack_status() {
    let addr = spawn_oracle().await;
    let mut c = OrderIntakeClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    let mut bad = order(1, WireSide::Bid, 5, 100, WireTif::Gtc);
    bad.client_order_id = vec![0u8; 8]; // wrong length
    let r = c.submit(bad).await.expect("submit").into_inner();
    let ack = r.ack.expect("ack");
    assert_ne!(ack.status, 0, "expected non-zero on bad input");
    assert!(r.fills.is_empty());
}
