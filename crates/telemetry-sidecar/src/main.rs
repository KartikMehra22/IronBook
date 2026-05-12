// Phase 2 telemetry-sidecar: tails the fairness-gateway's JSONL event log
// and echoes it to stderr. The real Redpanda producer lives in Phase 3.

use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let path = std::env::var("IRONBOOK_EVENT_LOG_PATH")
        .unwrap_or_else(|_| "/var/log/ironbook/events.jsonl".into());

    // The gateway may start a moment after us; poll until the file exists.
    let file = loop {
        match tokio::fs::File::open(&path).await {
            Ok(f) => break f,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(250)).await,
        }
    };
    let mut reader = tokio::io::BufReader::new(file);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        eprintln!("[telemetry-sidecar] {}", line.trim_end());
    }
}
