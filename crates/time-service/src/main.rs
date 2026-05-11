//! Time-service tonic gRPC server.
//!
//! Persists a high-watermark to `IRONBOOK_HWM_PATH` (env, default
//! `/var/lib/time-service/hw`) every second so restarts never collide with
//! sequence numbers already handed out.

#![allow(clippy::missing_errors_doc)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ironbook_proto::gen::ironbook::v1::time_service_server::{TimeService, TimeServiceServer};
use ironbook_proto::gen::ironbook::v1::{NextStampsRequest, NextStampsResponse};
use time_service::Clock;
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_BATCH_MAX: u32 = 65_535;
const SAFETY_GAP: u64 = 10_000;
const DEFAULT_HWM_PATH: &str = "/var/lib/time-service/hw";
const DEFAULT_ADDR: &str = "0.0.0.0:7070";

struct TimeServer {
    clock: Arc<Clock>,
}

#[tonic::async_trait]
impl TimeService for TimeServer {
    async fn next_stamps(
        &self,
        req: Request<NextStampsRequest>,
    ) -> Result<Response<NextStampsResponse>, Status> {
        let n = req.into_inner().batch_size.clamp(1, DEFAULT_BATCH_MAX);
        let (first_seq, first_ts) = self.clock.reserve(u64::from(n));
        Ok(Response::new(NextStampsResponse {
            first_seq,
            first_ts_ns: first_ts,
            step_ns: 100,
            batch_size: n,
        }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let hwm_path =
        std::env::var("IRONBOOK_HWM_PATH").unwrap_or_else(|_| DEFAULT_HWM_PATH.to_string());
    let addr_str = std::env::var("IRONBOOK_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());

    let starting = std::fs::read_to_string(&hwm_path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(SAFETY_GAP);

    let clock = Arc::new(Clock::new(starting));
    println!("time-service starting_seq={starting} addr={addr_str} hwm={hwm_path}");

    // Periodically persist the current high-watermark.
    let persister = {
        let c = clock.clone();
        let path = hwm_path.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some(parent) = std::path::Path::new(&path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&path, c.high_watermark().to_string());
            }
        })
    };

    let svc = TimeServer { clock };
    let addr = addr_str.parse().context("parse listen addr")?;
    Server::builder()
        .add_service(TimeServiceServer::new(svc))
        .serve(addr)
        .await
        .context("serve")?;

    persister.abort();
    Ok(())
}
