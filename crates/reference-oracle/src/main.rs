//! reference-oracle tonic gRPC server.

use anyhow::Context;
use ironbook_proto::gen::ironbook::v1::order_intake_server::OrderIntakeServer;
use reference_oracle::Oracle;
use tonic::transport::Server;

const DEFAULT_ADDR: &str = "0.0.0.0:7080";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr_str = std::env::var("IRONBOOK_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    println!("reference-oracle listening on {addr_str}");
    let addr = addr_str.parse().context("parse listen addr")?;
    Server::builder()
        .add_service(OrderIntakeServer::new(Oracle::new()))
        .serve(addr)
        .await
        .context("serve")?;
    Ok(())
}
