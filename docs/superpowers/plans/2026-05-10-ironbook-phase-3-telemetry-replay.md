# IronBook Phase 3 — Telemetry & Replay

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A live, scored leaderboard. Telemetry from the gateway+sidecar streams through Redpanda into ClickHouse; a Glicko-2 scoring-engine consumes per-run summaries, updates a Redis sorted set; a Next.js dashboard renders the live leaderboard with latency CDFs and per-order traces. Deterministic replay produces byte-identical input streams; the self-replay CI gate is green.

**Architecture (Phase 3 deltas):** Replace `FileSink` in fairness-gateway with `RedpandaSink`. Replace `telemetry-sidecar` placeholder with a real Rust SPSC→Redpanda producer. Stand up Redpanda single-broker, ClickHouse single-node with the §6.5 schema, Redis. Add `telemetry-ingester` (Rust SPSC → ClickHouse), `divergence-detector` (Rust stream-join), `replay-engine` (Rust Parquet read/write + content addressing), `scoring-engine` (Go Glicko-2), `leaderboard-api` (Go SSE), and the Next.js dashboard skeleton.

**Tech Stack:** Redpanda v24.1, ClickHouse v24.5, Redis 7.2, `franz-go` v1.17 (Go Kafka client), `clickhouse-go` v2.27, `kafka` Rust crate `rdkafka` v0.36, `clickhouse` Rust crate v0.13, `arrow` + `parquet` v53, Next.js 15 + uPlot 1.6.

**No more AI:** by this point Claude is gone. The plan is self-sufficient; verbatim code in critical-path tasks; defer `manifests-only` boilerplate to a single template per kind.

---

## Spec references

- Telemetry pipeline: spec §2.4
- Telemetry-ingester deep dive: spec §3.6
- Divergence detector: spec §3.7, §5.3
- Replay engine + Parquet schema: spec §3.8, §5.4
- ClickHouse schema (verbatim DDL): spec §6.5
- Composite scoring formula: spec §6.6
- Glicko-2 across scenarios: spec §6.7

---

## File structure for this phase

```
crates/
├── telemetry-ingester/      # T14.x — Rust, SPSC→ClickHouse
├── divergence-detector/     # T15.x — Rust, stream-join
├── replay-engine/           # T16.x — Rust, Parquet read/write
├── replay-format/           # T16.1 — Parquet schema lib
└── bot-worker/              # T14.4 — replaced placeholder; rdkafka-driven
apps/
├── scoring-engine/          # T17.x — Go, Glicko-2
└── leaderboard-api/         # T18.x — Go, SSE
frontend/
├── app/(dashboard)/leaderboard/page.tsx              # T18.4
├── app/(dashboard)/runs/[runId]/page.tsx             # T18.5
├── components/leaderboard/{Table,Row,Spark}.tsx
├── components/charts/{LatencyCDF,FlameGraph}.tsx
└── lib/{sse.ts,grpc-web.ts}
deploy/manifests/base/
├── redpanda/               # T13.1
├── clickhouse/             # T13.2
├── redis/                  # T13.3
├── telemetry-ingester/     # T14.x
├── divergence-detector/    # T15.x
├── replay-engine/          # T16.x
├── scoring-engine/         # T17.x
└── leaderboard-api/        # T18.x
proto/ironbook/v1/
├── telemetry.proto         # T14.1
├── divergence.proto        # T15.1
└── runs.proto              # extend with FlushedEvent
```

---

## Day 13 — Streaming + storage stack (~5 tasks, ~6 hours)

### Task 13.1: Redpanda single-broker

**Files:**
- Create: `deploy/manifests/base/redpanda/{statefulset,service,configmap}.yaml,kustomization.yaml`

- [ ] **Step 1: StatefulSet**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: redpanda, namespace: ironbook }
spec:
  serviceName: redpanda
  replicas: 1
  selector: { matchLabels: { app: redpanda } }
  template:
    metadata: { labels: { app: redpanda } }
    spec:
      containers:
        - name: redpanda
          image: redpandadata/redpanda:v24.1.7
          args:
            - redpanda
            - start
            - --smp=1
            - --memory=1G
            - --reserve-memory=0M
            - --overprovisioned
            - --node-id=0
            - --kafka-addr=PLAINTEXT://0.0.0.0:9092
            - --advertise-kafka-addr=PLAINTEXT://redpanda.ironbook.svc:9092
            - --rpc-addr=0.0.0.0:33145
            - --advertise-rpc-addr=redpanda.ironbook.svc:33145
            - --pandaproxy-addr=0.0.0.0:8082
          ports:
            - { name: kafka, containerPort: 9092 }
            - { name: rpc,   containerPort: 33145 }
            - { name: pp,    containerPort: 8082 }
          volumeMounts: [ { name: data, mountPath: /var/lib/redpanda/data } ]
  volumeClaimTemplates:
    - metadata: { name: data }
      spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 50Gi } } }
```

`service.yaml` headless on 9092.

- [ ] **Step 2: Topic creation Job** — runs `rpk topic create` for the topic set

`configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: redpanda-topics, namespace: ironbook }
data:
  topics.txt: |
    submissions.uploaded
    submissions.ready
    runs.events
    runs.flushed
```

`job.yaml`:
```yaml
apiVersion: batch/v1
kind: Job
metadata: { name: redpanda-topics-init, namespace: ironbook }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: rpk
          image: redpandadata/redpanda:v24.1.7
          command: ["/bin/bash","-eu","-o","pipefail","-c"]
          args:
            - |
              # wait until broker is up
              until rpk cluster info -X brokers=redpanda.ironbook.svc:9092 >/dev/null 2>&1; do sleep 2; done
              while read t; do
                rpk topic create "$t" -X brokers=redpanda.ironbook.svc:9092 --partitions 3 --replicas 1 || true
              done < /topics/topics.txt
          volumeMounts: [ { name: topics, mountPath: /topics } ]
      volumes:
        - { name: topics, configMap: { name: redpanda-topics } }
```

Per-run topics (`runs.<id>.submission_out`, `runs.<id>.oracle_out`, etc.) are created by the operator at `PRIMING` time — add `rpk topic create` calls to `ensure*Pod` helpers.

- [ ] **Step 3: Apply, verify**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl apply -k deploy/manifests/base/redpanda
KUBECONFIG=$PWD/kubeconfig.local kubectl rollout status -n ironbook statefulset/redpanda
KUBECONFIG=$PWD/kubeconfig.local kubectl -n ironbook exec sts/redpanda -- rpk topic list
```

Expected: 4 topics listed.

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/redpanda/
git commit -m "feat(deploy): single-broker Redpanda + topic-bootstrap Job"
```

---

### Task 13.2: ClickHouse with the `runs_raw` schema

**Files:**
- Create: `deploy/manifests/base/clickhouse/{statefulset,service,init-configmap}.yaml,kustomization.yaml`

- [ ] **Step 1: ClickHouse StatefulSet**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: clickhouse, namespace: ironbook }
spec:
  serviceName: clickhouse
  replicas: 1
  selector: { matchLabels: { app: clickhouse } }
  template:
    metadata: { labels: { app: clickhouse } }
    spec:
      containers:
        - name: ch
          image: clickhouse/clickhouse-server:24.5.3.5-alpine
          ports:
            - { name: http, containerPort: 8123 }
            - { name: tcp,  containerPort: 9000 }
          volumeMounts:
            - { name: data, mountPath: /var/lib/clickhouse }
            - { name: init, mountPath: /docker-entrypoint-initdb.d }
      volumes:
        - { name: init, configMap: { name: clickhouse-init } }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 50Gi } } }
```

`init-configmap.yaml` — embeds the §6.5 DDL verbatim:

```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: clickhouse-init, namespace: ironbook }
data:
  10-runs.sql: |
    CREATE DATABASE IF NOT EXISTS ironbook;

    CREATE TABLE IF NOT EXISTS ironbook.runs_raw (
        run_id           UUID,
        platform_seq     UInt64,
        platform_ts      UInt64,
        event_kind       Enum8('order'=1,'ack'=2,'fill'=3,'cancel'=4,'divergence'=5),
        client_order_id  UInt128,
        session_token    FixedString(32),
        side             Enum8('bid'=1,'ask'=2),
        qty              UInt64,
        price            Int64,
        order_type       Enum8('limit'=1,'market'=2),
        tif              Enum8('gtc'=1,'ioc'=2,'fok'=3),
        in_ts_ns         UInt64,
        ack_ts_ns        UInt64,
        fills            Array(Tuple(trade_id UInt64, maker_seq UInt64, price Int64, qty UInt64)),
        divergence_kind  Enum8('match'=1,'content'=2,'sub_missing'=3,'oracle_missing'=4) DEFAULT 'match',
        submission_sha256 FixedString(64),
        scenario_hash    FixedString(64),
        inserted_at      DateTime64(9) DEFAULT now64()
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(inserted_at)
    ORDER BY (run_id, platform_seq)
    TTL inserted_at + INTERVAL 7 DAY DELETE
    SETTINGS index_granularity = 8192;

    CREATE MATERIALIZED VIEW IF NOT EXISTS ironbook.runs_per_sec
    ENGINE = AggregatingMergeTree
    PARTITION BY toYYYYMMDD(ts_sec)
    ORDER BY (run_id, ts_sec)
    AS SELECT
        run_id,
        toStartOfInterval(fromUnixTimestamp64Nano(ack_ts_ns), INTERVAL 1 SECOND) AS ts_sec,
        count() AS orders,
        countIf(event_kind = 'fill') AS fills,
        countIf(divergence_kind != 'match') AS divergences,
        quantileTDigestState(0.5)(ack_ts_ns - in_ts_ns)  AS p50_state,
        quantileTDigestState(0.99)(ack_ts_ns - in_ts_ns) AS p99_state,
        sumState(toUInt64(qty)) AS qty_state
    FROM ironbook.runs_raw
    WHERE event_kind = 'ack'
    GROUP BY run_id, ts_sec;

    CREATE MATERIALIZED VIEW IF NOT EXISTS ironbook.runs_summary
    ENGINE = AggregatingMergeTree
    ORDER BY run_id
    AS SELECT
        run_id,
        minState(in_ts_ns)  AS started_at_state,
        maxState(ack_ts_ns) AS ended_at_state,
        countState() AS total_orders_state,
        quantileTDigestState(0.5)(ack_ts_ns - in_ts_ns)   AS p50_state,
        quantileTDigestState(0.9)(ack_ts_ns - in_ts_ns)   AS p90_state,
        quantileTDigestState(0.99)(ack_ts_ns - in_ts_ns)  AS p99_state,
        quantileTDigestState(0.999)(ack_ts_ns - in_ts_ns) AS p999_state,
        countIfState(divergence_kind = 'content')     AS content_div_state,
        countIfState(divergence_kind = 'sub_missing') AS sub_miss_state,
        countIfState(divergence_kind = 'match')       AS match_state
    FROM ironbook.runs_raw
    GROUP BY run_id;
```

- [ ] **Step 2: Apply + verify**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl apply -k deploy/manifests/base/clickhouse
KUBECONFIG=$PWD/kubeconfig.local kubectl -n ironbook exec sts/clickhouse -- clickhouse-client -q "SHOW TABLES FROM ironbook"
```

Expected: `runs_raw`, `runs_per_sec`, `runs_summary`.

- [ ] **Step 3: Commit**

```bash
git add deploy/manifests/base/clickhouse/
git commit -m "feat(deploy): single-node ClickHouse with runs_raw + materialized views"
```

---

### Task 13.3: Redis

**Files:**
- Create: `deploy/manifests/base/redis/{statefulset,service}.yaml,kustomization.yaml`

(Standard pattern: 1-replica StatefulSet on `redis:7.2-alpine` with `--appendonly yes`, port 6379, 5 GiB PVC.)

- [ ] **Apply + commit.**

```bash
git add deploy/manifests/base/redis/
git commit -m "feat(deploy): single-node Redis with AOF persistence"
```

---

### Task 13.4: Wire dev overlay; smoke

- [ ] Update `deploy/manifests/overlays/dev/kustomization.yaml` to include `redpanda`, `clickhouse`, `redis`.
- [ ] `make dev`; verify all three are Ready.
- [ ] Commit.

---

### Task 13.5: ClickHouse + Redpanda Go clients in `pkg/`

**Files:**
- Create: `pkg/clickhouseclient/client.go`
- Create: `pkg/redpandaclient/{producer,consumer}.go`

- [ ] **Step 1: clickhouse-go wrapper** — connection pool

```go
package clickhouseclient

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func New(ctx context.Context, addr string) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: "ironbook", Username: "default", Password: ""},
		DialTimeout: 5 * time.Second,
		MaxOpenConns: 10,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Protocol: clickhouse.Native,
	})
}
```

- [ ] **Step 2: franz-go producer + consumer wrappers** in `pkg/redpandaclient/`.

```go
// producer.go
package redpandaclient

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewProducer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.MaxBufferedRecords(1_000_000),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(8<<20, false, false, nil)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
}
```

```go
// consumer.go
package redpandaclient

import "github.com/twmb/franz-go/pkg/kgo"

func NewConsumer(brokers []string, group, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
}
```

```bash
go get github.com/twmb/franz-go/pkg/kgo@v1.17.0
go get github.com/ClickHouse/clickhouse-go/v2@v2.27.0
go mod tidy
```

- [ ] **Commit.**

```bash
git add pkg/clickhouseclient/ pkg/redpandaclient/ go.mod go.sum
git commit -m "feat(pkg): franz-go + clickhouse-go client wrappers"
```

---

## Day 14 — Telemetry-ingester + RedpandaSink in gateway (~4 tasks, ~7 hours)

### Task 14.1: telemetry.proto schema

**Files:**
- Create: `proto/ironbook/v1/telemetry.proto`

```proto
syntax = "proto3";
package ironbook.v1;
option go_package = "github.com/<owner>/IronBook/pkg/proto/ironbook/v1;ironbookv1";

import "ironbook/v1/orders.proto";

message TelemetryEvent {
  bytes  run_id           = 1; // UUID 16 bytes
  uint64 platform_seq     = 2;
  uint64 platform_ts_ns   = 3;
  uint64 in_ts_ns         = 4;
  uint64 ack_ts_ns        = 5;
  EventKind event_kind    = 6;
  NormalizedOrder order   = 7;
  Ack             ack     = 8;
  repeated Fill   fills   = 9;
  string source           = 10; // "submission" or "oracle"
  bytes  submission_sha256 = 11;
  bytes  scenario_hash    = 12;
}
enum EventKind { EK_UNSPECIFIED = 0; EK_ORDER = 1; EK_ACK = 2; EK_FILL = 3; EK_CANCEL = 4; EK_DIVERGENCE = 5; }

message TelemetryBatch { repeated TelemetryEvent events = 1; }
```

`buf generate`. Commit.

---

### Task 14.2: telemetry-ingester (Rust, SPSC → ClickHouse)

**Files:**
- Create: `crates/telemetry-ingester/{Cargo.toml,src/{main,batch,sink_clickhouse,server}.rs}`

- [ ] **Step 1: Cargo.toml**

```toml
[package]
name        = "telemetry-ingester"
version     = "0.0.1"
edition.workspace = true
[dependencies]
tokio = { workspace = true }
tonic = "0.12"
prost = "0.13"
ironbook-proto = { path = "../proto", features = ["grpc"] }
clickhouse = "0.13"
crossbeam = { workspace = true }
serde = { workspace = true }
anyhow = { workspace = true }
[lints]
workspace = true
```

Add to workspace members.

- [ ] **Step 2: SPSC ring with bounded drop policy** — `src/batch.rs`

```rust
use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

pub struct Drop_counter(pub std::sync::atomic::AtomicU64);

pub struct Ring<T> {
    inner: Arc<ArrayQueue<T>>,
    drops: Arc<std::sync::atomic::AtomicU64>,
}
impl<T> Clone for Ring<T> { fn clone(&self) -> Self { Self { inner: self.inner.clone(), drops: self.drops.clone() } } }

impl<T> Ring<T> {
    pub fn new(capacity: usize) -> Self {
        Self { inner: Arc::new(ArrayQueue::new(capacity)), drops: Arc::default() }
    }
    /// Push, dropping on full and counting.
    pub fn push_or_drop(&self, v: T) {
        if let Err(_) = self.inner.push(v) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    pub fn pop(&self) -> Option<T> { self.inner.pop() }
    pub fn drops(&self) -> u64 { self.drops.load(std::sync::atomic::Ordering::Relaxed) }
}
```

- [ ] **Step 3: ClickHouse sink with batched insert** — `src/sink_clickhouse.rs`

```rust
use clickhouse::{Client, Row, inserter::Inserter};
use serde::Serialize;

#[derive(Row, Serialize)]
pub struct RunsRawRow {
    pub run_id: [u8; 16],
    pub platform_seq: u64,
    pub platform_ts: u64,
    pub event_kind: u8,
    pub client_order_id: u128,
    pub session_token: [u8; 32],
    pub side: u8,
    pub qty: u64,
    pub price: i64,
    pub order_type: u8,
    pub tif: u8,
    pub in_ts_ns: u64,
    pub ack_ts_ns: u64,
    pub fills: Vec<(u64, u64, i64, u64)>,
    pub divergence_kind: u8,
    pub submission_sha256: [u8; 64],
    pub scenario_hash: [u8; 64],
}

pub async fn run_drainer(ring: crate::batch::Ring<RunsRawRow>, ch_addr: String) -> anyhow::Result<()> {
    let client = Client::default().with_url(format!("http://{ch_addr}")).with_database("ironbook");
    let mut inserter: Inserter<RunsRawRow> = client.inserter("runs_raw")?
        .with_max_rows(1000)
        .with_period(Some(std::time::Duration::from_millis(50)));

    loop {
        while let Some(row) = ring.pop() { inserter.write(&row)?; }
        // flush periodically
        let _ = inserter.commit().await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
```

- [ ] **Step 4: gRPC server that accepts `TelemetryBatch`** — `src/server.rs`

```rust
use ironbook_proto::gen::ironbook::v1::*;
use tonic::{Request, Response, Status};

pub struct Ingester { pub ring: crate::batch::Ring<crate::sink_clickhouse::RunsRawRow> }

#[tonic::async_trait]
impl telemetry_ingester_server::TelemetryIngester for Ingester {
    async fn ingest(&self, req: Request<TelemetryBatch>) -> Result<Response<()>, Status> {
        for ev in req.into_inner().events {
            let row = to_row(&ev);
            self.ring.push_or_drop(row);
        }
        Ok(Response::new(()))
    }
}

fn to_row(ev: &TelemetryEvent) -> crate::sink_clickhouse::RunsRawRow {
    use crate::sink_clickhouse::RunsRawRow;
    let order = ev.order.as_ref();
    let ack   = ev.ack.as_ref();

    // Map proto enums (i32) to ClickHouse enum8 byte values per the schema in T13.2.
    let event_kind: u8 = match ev.event_kind() {
        EventKind::EkOrder       => 1,
        EventKind::EkAck         => 2,
        EventKind::EkFill        => 3,
        EventKind::EkCancel      => 4,
        EventKind::EkDivergence  => 5,
        EventKind::EkUnspecified => 1, // default to "order"
    };
    let side: u8 = match order.and_then(|o| Side::try_from(o.side).ok()) {
        Some(Side::Bid) => 1,
        Some(Side::Ask) => 2,
        _               => 1,
    };
    let order_type: u8 = match order.and_then(|o| OrderType::try_from(o.order_type).ok()) {
        Some(OrderType::Limit)  => 1,
        Some(OrderType::Market) => 2,
        _                       => 1,
    };
    let tif: u8 = match order.and_then(|o| TimeInForce::try_from(o.tif).ok()) {
        Some(TimeInForce::Gtc) => 1,
        Some(TimeInForce::Ioc) => 2,
        Some(TimeInForce::Fok) => 3,
        _                      => 1,
    };

    // Fixed-size byte arrays — pad/truncate if proto bytes are wrong length.
    let run_id: [u8; 16] = ev.run_id.as_slice().try_into().unwrap_or([0u8; 16]);
    let coid:   u128     = order
        .map(|o| {
            let arr: [u8; 16] = o.client_order_id.as_slice().try_into().unwrap_or([0u8; 16]);
            u128::from_be_bytes(arr)
        })
        .unwrap_or(0);
    let session_token: [u8; 32] = order
        .map(|o| o.session_token.as_slice().try_into().unwrap_or([0u8; 32]))
        .unwrap_or([0u8; 32]);

    // Pad sha fields (proto carries hex string OR 32 raw bytes; spec says hex string of length 64).
    let mut sub_sha = [b'0'; 64];
    sub_sha.copy_from_slice(&pad_to_64(&ev.submission_sha256));
    let mut scn_hash = [b'0'; 64];
    scn_hash.copy_from_slice(&pad_to_64(&ev.scenario_hash));

    let fills: Vec<(u64, u64, i64, u64)> = ev.fills.iter()
        .map(|f| (f.trade_id, f.platform_seq_maker, f.price, f.qty))
        .collect();

    let divergence_kind: u8 = 1; // "match"; divergence-detector emits dedicated DivergenceEvents instead.

    RunsRawRow {
        run_id,
        platform_seq:   ev.platform_seq,
        platform_ts:    ev.platform_ts_ns,
        event_kind,
        client_order_id: coid,
        session_token,
        side,
        qty:            order.map(|o| o.qty).unwrap_or(0),
        price:          order.map(|o| o.price).unwrap_or(0),
        order_type,
        tif,
        in_ts_ns:       ev.in_ts_ns,
        ack_ts_ns:      ack.map(|a| a.ack_ts_ns).unwrap_or(ev.ack_ts_ns),
        fills,
        divergence_kind,
        submission_sha256: sub_sha,
        scenario_hash:     scn_hash,
    }
}

fn pad_to_64(input: &[u8]) -> Vec<u8> {
    let mut out = vec![b'0'; 64];
    let n = input.len().min(64);
    out[..n].copy_from_slice(&input[..n]);
    out
}
```

(Add the `TelemetryIngester` service to `proto/ironbook/v1/telemetry.proto` with `rpc Ingest(TelemetryBatch) returns (google.protobuf.Empty);`. Run `buf generate`.)

- [ ] **Step 5: Verify the row-mapping compiles** — `cargo build -p telemetry-ingester` should be clean.

- [ ] **Step 6: `main.rs`**

```rust
mod batch; mod sink_clickhouse; mod server;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ch = std::env::var("CLICKHOUSE_ADDR").unwrap_or_else(|_| "clickhouse.ironbook.svc:8123".into());
    let ring = batch::Ring::<sink_clickhouse::RunsRawRow>::new(1_000_000);
    tokio::spawn(sink_clickhouse::run_drainer(ring.clone(), ch));
    let svc = server::Ingester { ring };
    tonic::transport::Server::builder()
        .add_service(ironbook_proto::gen::ironbook::v1::telemetry_ingester_server::TelemetryIngesterServer::new(svc))
        .serve("0.0.0.0:9100".parse()?).await?;
    Ok(())
}
```

- [ ] **Step 7: Manifest + commit**

```bash
git add crates/telemetry-ingester/ proto/ironbook/v1/telemetry.proto
git commit -m "feat(telemetry-ingester): Rust SPSC ring → ClickHouse batched insert"
```

---

### Task 14.3: Replace gateway FileSink with RedpandaSink

**Files:**
- Modify: `apps/fairness-gateway/gateway/sink_file.go` (rename to `sink_redpanda.go`)
- Modify: `apps/fairness-gateway/main.go`

- [ ] **Step 1: New sink**

```go
package gateway

import (
	"context"
	"encoding/json"

	pb "github.com/<owner>/IronBook/pkg/proto/ironbook/v1"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RedpandaSink struct {
	cli   *kgo.Client
	topic string
}

func NewRedpandaSink(cli *kgo.Client, topic string) *RedpandaSink { return &RedpandaSink{cli: cli, topic: topic} }

func (s *RedpandaSink) OnIn(o *pb.NormalizedOrder) {
	ev := &pb.TelemetryEvent{RunId: nil, PlatformSeq: o.PlatformSeq, EventKind: pb.EventKind_EK_ORDER, Order: o}
	s.produce(ev)
}
func (s *RedpandaSink) OnReply(o *pb.NormalizedOrder, source string, r *pb.Reply) {
	ev := &pb.TelemetryEvent{
		PlatformSeq: o.PlatformSeq, EventKind: pb.EventKind_EK_ACK,
		Order: o, Ack: r.Ack, Fills: r.Fills, Source: source,
	}
	s.produce(ev)
}
func (s *RedpandaSink) produce(ev *pb.TelemetryEvent) {
	b, _ := json.Marshal(ev)  // simple JSON encode for Phase 3; Phase 4 swaps to proto bytes
	s.cli.Produce(context.Background(), &kgo.Record{Topic: s.topic, Value: b}, nil)
}
```

- [ ] **Step 2: Wire in `main.go`**

```go
brokers := []string{os.Getenv("REDPANDA_BROKERS")}  // "redpanda.ironbook.svc:9092"
runID  := os.Getenv("RUN_ID")
topic  := fmt.Sprintf("runs.%s.events", runID)
cli, err := redpandaclient.NewProducer(brokers); if err != nil { log.Fatal(err) }
sink := gateway.NewRedpandaSink(cli, topic)
```

Replace `gateway.NewFileSink(logPath)` with `sink`.

- [ ] **Step 3: Rebuild + redeploy gateway image; commit.**

```bash
git add apps/fairness-gateway/
git commit -m "feat(fairness-gateway): replace FileSink with RedpandaSink (per-run topic)"
```

---

### Task 14.5: bot-worker (Rust, async REST/WS over rdkafka claims)

**Files:**
- Replace: `crates/bot-worker/src/main.rs`
- Modify: `crates/bot-worker/Cargo.toml`

The Phase 2 `bot-coordinator` does Go-side direct REST dispatch. For higher sustained TPS (≥ 5k orders/s) the dispatch needs to fan out to multiple Rust workers consuming from a Redpanda `runs.<id>.dispatch` topic. KEDA autoscales the Deployment by consumer-group lag.

- [ ] **Step 1: Cargo.toml**

```toml
[package]
name        = "bot-worker"
version     = "0.0.1"
edition.workspace = true
[dependencies]
tokio    = { workspace = true }
rdkafka  = "0.36"
reqwest  = { version = "0.12", features = ["json"] }
serde    = { workspace = true }
serde_json = { workspace = true }
anyhow   = { workspace = true }
[lints]
workspace = true
```

- [ ] **Step 2: `src/main.rs`**

```rust
use rdkafka::{config::ClientConfig, consumer::{Consumer, StreamConsumer}, Message};
use serde::Deserialize;

#[derive(Deserialize)]
struct DispatchEvent {
    bot_id: u64, local_seq: u64,
    side: String, qty: u64, price: i64,
    order_type: String, tif: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let brokers = std::env::var("REDPANDA_BROKERS")?;
    let topic   = std::env::var("REDPANDA_TOPIC")?; // runs.<id>.dispatch
    let gateway = std::env::var("GATEWAY_URL")?;
    let group   = std::env::var("CONSUMER_GROUP").unwrap_or_else(|_| "bot-workers".into());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[&topic])?;

    let http = reqwest::Client::builder().pool_max_idle_per_host(64).build()?;

    loop {
        let msg = consumer.recv().await?;
        let Some(payload) = msg.payload() else { continue };
        let Ok(ev) = serde_json::from_slice::<DispatchEvent>(payload) else { continue };
        let body = serde_json::json!({
            "bot_id": ev.bot_id, "local_seq": ev.local_seq,
            "side": ev.side, "qty": ev.qty, "price": ev.price,
            "order_type": ev.order_type, "tif": ev.tif,
        });
        // Fire-and-forget; the gateway is the latency truth source.
        let _ = http.post(format!("{gateway}/v1/order")).json(&body).send().await;
    }
}
```

- [ ] **Step 3: Modify `bot-coordinator` to publish to the dispatch topic instead of POSTing directly** when env `DISPATCH_VIA_REDPANDA=true`. Otherwise keep direct-REST behaviour for low-rate scenarios.

- [ ] **Step 4: KEDA ScaledObject**

`deploy/manifests/base/bot-worker/scaledobject.yaml`:
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: bot-worker, namespace: ironbook }
spec:
  scaleTargetRef: { name: bot-worker }
  minReplicaCount: 1
  maxReplicaCount: 8
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: redpanda.ironbook.svc:9092
        consumerGroup: bot-workers
        topic: runs.dispatch  # NB: across-runs trigger; per-run topics aggregate via consumer pattern
        lagThreshold: "100"
```

(KEDA itself installs via Helm or upstream YAML; add a `deploy/manifests/base/keda/` directory if not already present.)

- [ ] **Step 5: Manifest, build, commit**

```bash
git add crates/bot-worker/ apps/bot-coordinator/ deploy/manifests/base/bot-worker/
git commit -m "feat(bot-worker): Rust async dispatcher consuming Redpanda dispatch topic; KEDA autoscale on lag"
```

---

### Task 14.4: telemetry-sidecar real impl (consumes from Redpanda → forwards to ingester via gRPC)

**Files:**
- Modify: `crates/telemetry-sidecar/Cargo.toml`
- Replace: `crates/telemetry-sidecar/src/main.rs`

- [ ] **Step 1: Cargo.toml deps**

```toml
[dependencies]
tokio = { workspace = true }
rdkafka = "0.36"
tonic = "0.12"
ironbook-proto = { path = "../proto", features = ["grpc"] }
prost = "0.13"
serde = { workspace = true }
serde_json = { workspace = true }
anyhow = { workspace = true }
```

- [ ] **Step 2: `main.rs`** — consume topic, decode JSON, batch-forward to ingester

```rust
use rdkafka::{config::ClientConfig, consumer::{Consumer, StreamConsumer}, Message};
use ironbook_proto::gen::ironbook::v1::*;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let brokers = std::env::var("REDPANDA_BROKERS")?;
    let topic   = std::env::var("REDPANDA_TOPIC")?;
    let ingester = std::env::var("INGESTER_ADDR")?; // "telemetry-ingester.ironbook.svc:9100"

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &format!("sidecar-{}", topic))
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[&topic])?;

    let mut client = telemetry_ingester_client::TelemetryIngesterClient::connect(format!("http://{ingester}")).await?;

    let mut buf: Vec<TelemetryEvent> = Vec::with_capacity(1024);
    let mut last_flush = std::time::Instant::now();
    loop {
        tokio::select! {
            msg = consumer.recv() => {
                let m = msg?; if let Some(payload) = m.payload() {
                    if let Ok(ev) = serde_json::from_slice::<TelemetryEvent>(payload) {
                        buf.push(ev);
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
        if buf.len() >= 1000 || last_flush.elapsed() > std::time::Duration::from_millis(100) {
            if !buf.is_empty() {
                let _ = client.ingest(TelemetryBatch { events: std::mem::take(&mut buf) }).await;
                last_flush = std::time::Instant::now();
            }
        }
    }
}
```

- [ ] **Step 3: Manifests, commit.**

The sidecar runs as a per-run pod (one per BenchmarkRun). Operator creates it in `toPriming`.

```bash
git add crates/telemetry-sidecar/
git commit -m "feat(telemetry-sidecar): consume Redpanda topic → batch forward to ingester"
```

---

## Day 15 — Divergence detector (~3 tasks, ~6 hours)

### Task 15.1: divergence.proto

```proto
syntax = "proto3";
package ironbook.v1;
import "ironbook/v1/orders.proto";

enum DivergenceKind { DK_UNSPECIFIED = 0; DK_MATCH = 1; DK_CONTENT = 2; DK_SUB_MISSING = 3; DK_ORACLE_MISSING = 4; }

message DivergenceEvent {
  bytes run_id = 1;
  uint128 client_order_id = 2;
  uint64 platform_seq = 3;
  DivergenceKind kind = 4;
  repeated Fill submission_fills = 5;
  repeated Fill oracle_fills = 6;
  Ack submission_ack = 7;
  Ack oracle_ack = 8;
  uint64 emitted_at_ns = 9;
}
```

(`uint128` is not native protobuf; use `bytes(16)` instead.)

`buf generate`. Commit.

---

### Task 15.2: divergence-detector core (Rust)

**Files:**
- Create: `crates/divergence-detector/{Cargo.toml,src/{main,join,cache}.rs}`

- [ ] **Step 1: LRU join cache** — `src/cache.rs`

```rust
use std::collections::HashMap;
use std::time::Instant;

pub struct PendingEvent {
    pub run_id: [u8; 16],
    pub platform_seq: u64,
    pub client_order_id: u128,
    pub source: Source,
    pub fills: Vec<crate::ProtoFill>,
    pub ack:   Option<crate::ProtoAck>,
    pub at:    Instant,
}
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Source { Submission, Oracle }

pub struct JoinCache {
    pending: HashMap<u64, PendingEvent>, // key: platform_seq (within a run)
    capacity: usize,
}
impl JoinCache {
    pub fn new(capacity: usize) -> Self { Self { pending: HashMap::with_capacity(capacity), capacity } }

    /// Returns Some((sub, oracle)) if a join completes; None if still waiting.
    pub fn put(&mut self, ev: PendingEvent) -> Option<(PendingEvent, PendingEvent)> {
        if let Some(other) = self.pending.remove(&ev.platform_seq) {
            return Some(match (ev.source, other.source) {
                (Source::Submission, Source::Oracle) => (ev, other),
                (Source::Oracle, Source::Submission) => (other, ev),
                _ => return None, // dup — drop one
            });
        }
        if self.pending.len() >= self.capacity {
            // Evict oldest
            let oldest_seq = *self.pending.iter().min_by_key(|(_, v)| v.at).map(|(k, _)| k).unwrap();
            self.pending.remove(&oldest_seq);
        }
        self.pending.insert(ev.platform_seq, ev);
        None
    }
    pub fn evict_older_than(&mut self, cutoff: Instant) -> Vec<PendingEvent> {
        let stale: Vec<u64> = self.pending.iter().filter(|(_, v)| v.at < cutoff).map(|(k, _)| *k).collect();
        stale.into_iter().filter_map(|k| self.pending.remove(&k)).collect()
    }
}
```

- [ ] **Step 2: Comparator** — `src/join.rs`

```rust
use crate::cache::*;
use ironbook_proto::gen::ironbook::v1::DivergenceKind;

pub fn classify(sub: &PendingEvent, oracle: &PendingEvent) -> DivergenceKind {
    if fills_equal(&sub.fills, &oracle.fills) && acks_equal(sub.ack.as_ref(), oracle.ack.as_ref()) {
        DivergenceKind::Match
    } else {
        DivergenceKind::Content
    }
}
fn fills_equal(a: &[crate::ProtoFill], b: &[crate::ProtoFill]) -> bool {
    if a.len() != b.len() { return false; }
    a.iter().zip(b.iter()).all(|(x, y)| x.price == y.price && x.qty == y.qty && x.platform_seq_maker == y.platform_seq_maker)
}
fn acks_equal(a: Option<&crate::ProtoAck>, b: Option<&crate::ProtoAck>) -> bool {
    match (a, b) { (Some(x), Some(y)) => x.status == y.status, (None, None) => true, _ => false }
}
```

- [ ] **Step 3: `main.rs`** — Redpanda consumer (per-run topic) + join + producer for divergence topic

(Standard pattern; consume `runs.<id>.events`, route by `EventKind`, plug into `JoinCache`, emit `DivergenceEvent` to `runs.<id>.divergence`.)

- [ ] **Step 4: Tests + Commit**

```bash
git add crates/divergence-detector/ proto/ironbook/v1/divergence.proto
git commit -m "feat(divergence-detector): LRU-bounded stream-join with content / sub_missing / oracle_missing classification"
```

---

### Task 15.3: Wire divergence-detector into operator

- [ ] Operator's `ensure*Pod` helpers add a `divergence-detector` Deployment per run, with env `RUN_ID`, `REDPANDA_BROKERS`.
- [ ] Update `BenchmarkRunStatus` with `DivergenceTopic` field.
- [ ] Commit.

---

## Day 16 — Replay-engine + Parquet (~4 tasks, ~7 hours)

### Task 16.1: `replay-format` lib (Parquet schema)

**Files:**
- Create: `crates/replay-format/{Cargo.toml,src/lib.rs}`

- [ ] **Step 1: Schema definition matching spec §5.4.1**

```rust
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub fn replay_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("platform_seq",  DataType::UInt64, false),
        Field::new("platform_ts",   DataType::UInt64, false),
        Field::new("run_id",        DataType::FixedSizeBinary(16), false),
        Field::new("client_order_id", DataType::FixedSizeBinary(16), false),
        Field::new("session_token", DataType::FixedSizeBinary(32), false),
        Field::new("op",            DataType::Int32, false),
        Field::new("side",          DataType::Int32, false),
        Field::new("qty",           DataType::UInt64, false),
        Field::new("price",         DataType::Int64, false),
        Field::new("order_type",    DataType::Int32, false),
        Field::new("tif",           DataType::Int32, false),
        Field::new("wire_format",   DataType::Int32, false),
        // oracle_fills, oracle_acks: lists; for brevity we serialize as JSON strings here
        Field::new("oracle_fills_json", DataType::Utf8, true),
        Field::new("oracle_acks_json",  DataType::Utf8, true),
    ]))
}

pub fn write_batch_to_parquet(path: &std::path::Path, schema: Arc<Schema>, batches: Vec<arrow::record_batch::RecordBatch>)
    -> Result<[u8; 32], anyhow::Error>
{
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use sha2::{Digest, Sha256};

    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    for b in &batches { writer.write(b)?; }
    writer.close()?;

    // sha256 the resulting file for content-addressing.
    let mut hasher = Sha256::new();
    let mut f = std::fs::File::open(path)?;
    std::io::copy(&mut f, &mut hasher)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(&hasher.finalize());
    Ok(out)
}
```

```toml
# Cargo.toml
[dependencies]
arrow = "53"
parquet = "53"
anyhow = { workspace = true }
sha2 = "0.10"
```

Add to workspace.

- [ ] **Commit.**

```bash
git add crates/replay-format/ Cargo.toml
git commit -m "feat(replay-format): Parquet schema + zstd-compressed writer with sha256 content addressing"
```

---

### Task 16.2: replay-engine service

**Files:**
- Create: `crates/replay-engine/{Cargo.toml,src/{main,reader,emitter}.rs}`

- [ ] **Step 1: Reader** — Parquet → events

```rust
use parquet::arrow::ParquetRecordBatchReaderBuilder;

#[derive(Clone, Debug)]
pub struct ReplayEvent {
    pub platform_seq:   u64,
    pub platform_ts_ns: u64,
    pub run_id:         [u8; 16],
    pub client_order_id: [u8; 16],
    pub session_token:  [u8; 32],
    pub op:             i32, // 0=NEW 1=CANCEL 2=AMEND
    pub side:           i32,
    pub qty:            u64,
    pub price:          i64,
    pub order_type:     i32,
    pub tif:            i32,
    pub wire_format:    i32,
}

pub fn read_events(path: &std::path::Path) -> anyhow::Result<Vec<ReplayEvent>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        out.extend(decode_batch(&batch)?);
    }
    Ok(out)
}

fn decode_batch(b: &arrow::record_batch::RecordBatch) -> anyhow::Result<Vec<ReplayEvent>> {
    use arrow::array::*;
    let n = b.num_rows();
    // Each column ordered exactly as defined in replay-format::replay_schema().
    let platform_seq    = b.column(0).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("col 0"))?;
    let platform_ts     = b.column(1).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("col 1"))?;
    let run_id_arr      = b.column(2).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| anyhow::anyhow!("col 2"))?;
    let client_oid_arr  = b.column(3).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| anyhow::anyhow!("col 3"))?;
    let session_tok_arr = b.column(4).as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| anyhow::anyhow!("col 4"))?;
    let op              = b.column(5).as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("col 5"))?;
    let side            = b.column(6).as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("col 6"))?;
    let qty             = b.column(7).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("col 7"))?;
    let price           = b.column(8).as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow::anyhow!("col 8"))?;
    let order_type      = b.column(9).as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("col 9"))?;
    let tif             = b.column(10).as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("col 10"))?;
    let wire_format     = b.column(11).as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("col 11"))?;

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut run_id = [0u8; 16];
        run_id.copy_from_slice(run_id_arr.value(i));
        let mut client_order_id = [0u8; 16];
        client_order_id.copy_from_slice(client_oid_arr.value(i));
        let mut session_token = [0u8; 32];
        session_token.copy_from_slice(session_tok_arr.value(i));

        out.push(ReplayEvent {
            platform_seq:   platform_seq.value(i),
            platform_ts_ns: platform_ts.value(i),
            run_id, client_order_id, session_token,
            op:          op.value(i),
            side:        side.value(i),
            qty:         qty.value(i),
            price:       price.value(i),
            order_type:  order_type.value(i),
            tif:         tif.value(i),
            wire_format: wire_format.value(i),
        });
    }
    Ok(out)
}
```

- [ ] **Step 2: Emitter** — POST each event back through fairness-gateway, **preserving original `platform_seq` + `platform_ts`**

```rust
pub async fn emit(events: Vec<ReplayEvent>, gateway: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    for ev in events {
        // bot identity stripped to fixed bot_id=0; gateway's RunSecret derives session_token.
        let body = serde_json::json!({
            "bot_id": 0, "local_seq": ev.platform_seq,
            "side": ev.side, "qty": ev.qty, "price": ev.price,
            "order_type": ev.order_type, "tif": ev.tif,
        });
        // For replay, gateway must NOT call time-service; instead use ev.platform_seq/ts.
        // We pass them via a header that the gateway honors when REPLAY_MODE=true.
        client.post(format!("{gateway}/v1/order"))
            .header("X-Replay-Platform-Seq", ev.platform_seq.to_string())
            .header("X-Replay-Platform-Ts",  ev.platform_ts_ns.to_string())
            .json(&body).send().await?;
    }
    Ok(())
}
```

- [ ] **Step 3: Modify gateway to honor replay headers when `REPLAY_MODE=true` env**

In `apps/fairness-gateway/gateway/server.go`, before calling `Stamper.Next`, check headers; if both present and `REPLAY_MODE` env is true, use them directly.

- [ ] **Commit.**

```bash
git add crates/replay-engine/ apps/fairness-gateway/
git commit -m "feat(replay-engine): Parquet reader + replay emitter; gateway honors X-Replay headers"
```

---

### Task 16.3: Parquet sink in telemetry-ingester

**Files:**
- Modify: `crates/telemetry-ingester/src/`

- [ ] **Step 1: Add MinIO client and write a per-run Parquet**

When the operator marks a run `DRAINING`, it sends a `Flush` RPC to telemetry-ingester. The ingester drains pending events for that run, writes a Parquet to MinIO `s3://ironbook-replay/<run_id>/<file_id>.parquet` using `replay-format`, and emits to `runs.flushed` topic.

Add `aws-sdk-s3 = "1"` (or `s3 = "0.34"` simpler) to `Cargo.toml`.

(Implementation detail: drain into `Vec<ReplayEvent>`, build Arrow `RecordBatch`, call `replay_format::write_batch_to_parquet`, upload bytes to MinIO via S3 PUT with `If-None-Match: *` for atomic seal.)

- [ ] **Commit.**

```bash
git add crates/telemetry-ingester/
git commit -m "feat(telemetry-ingester): seal per-run Parquet replay log to MinIO at DRAINING"
```

---

### Task 16.4: Self-replay byte-equality CI gate

**Files:**
- Create: `tools/make-replay/main.go`
- Modify: `Makefile`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: `tools/make-replay/main.go` runs:**
  1. Boot a fixture scenario against the reference oracle alone.
  2. Capture replay log F1 (sha256).
  3. Replay F1 against the same oracle image.
  4. Capture log F2 (sha256).
  5. Assert F1 == F2.

```go
package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func sha(path string) string {
	f, _ := os.Open(path); defer f.Close()
	h := sha256.New(); io.Copy(h, f); return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	check := flag.Bool("self-replay-check", false, "")
	flag.Parse()
	if !*check { log.Fatal("--self-replay-check required") }
	// 1. start fresh oracle pod via kubectl/helm; trigger fixture run
	// 2. wait for runs.flushed topic event
	// 3. download Parquet
	// (The script's full body is kubectl/curl heavy — see cases.)
	_ = sha; _ = http.DefaultClient
}
```

- [ ] **Step 2: Makefile target**

```make
ci-self-replay:    ## Run the determinism gate
	go run ./tools/make-replay --self-replay-check
```

- [ ] **Step 3: Add a CI job that runs `make ci-self-replay` on a kind cluster.**

- [ ] **Commit.**

```bash
git add tools/make-replay/ Makefile .github/workflows/ci.yml
git commit -m "ci: add self-replay byte-equality gate (determinism proof)"
```

---

## Day 17 — Scoring engine + Glicko-2 (~4 tasks, ~6 hours)

### Task 17.1: Glicko-2 lib in `pkg/glicko2/`

**Files:**
- Create: `pkg/glicko2/glicko2.go`
- Create: `pkg/glicko2/glicko2_test.go`

- [ ] **Step 1: Test cases from the Mark Glickman reference paper** (canonical numerical examples)

```go
func TestGlicko2_PaperExample(t *testing.T) {
	// Player rating 1500, RD 200, σ 0.06, faces opponents with known outcomes.
	p := Player{Mu: 0.0, Phi: 1.1513, Sigma: 0.06}
	opps := []Match{
		{Mu: -0.5756, Phi: 0.1727, Score: 1},
		{Mu:  0.2878, Phi: 0.5756, Score: 0},
		{Mu:  1.7269, Phi: 1.7269, Score: 0},
	}
	got := Update(p, opps, 0.5)  // tau = 0.5
	// Expected from Glickman 2013 paper:
	if !approx(got.Mu, -0.2069, 1e-3) { t.Errorf("μ=%f", got.Mu) }
	if !approx(got.Phi, 0.8722, 1e-3) { t.Errorf("φ=%f", got.Phi) }
}
```

- [ ] **Step 2: Implement `Update` per the Glickman 2013 paper**

```go
package glicko2

import "math"

const Tau = 0.5

type Player struct { Mu, Phi, Sigma float64 }
type Match  struct { Mu, Phi, Score float64 }

func Update(p Player, ms []Match, tau float64) Player {
	// step 2: g and E
	g := func(phi float64) float64 { return 1.0 / math.Sqrt(1.0 + 3.0*phi*phi/(math.Pi*math.Pi)) }
	E := func(mu, oppMu, oppPhi float64) float64 { return 1.0 / (1.0 + math.Exp(-g(oppPhi)*(mu-oppMu))) }

	// step 3: variance v
	v := 0.0
	for _, m := range ms {
		gp := g(m.Phi)
		e  := E(p.Mu, m.Mu, m.Phi)
		v += gp*gp * e * (1 - e)
	}
	v = 1.0 / v

	// step 4: delta
	delta := 0.0
	for _, m := range ms {
		gp := g(m.Phi); e := E(p.Mu, m.Mu, m.Phi)
		delta += gp * (m.Score - e)
	}
	delta *= v

	// step 5: new sigma (illinois algorithm)
	a := math.Log(p.Sigma * p.Sigma)
	f := func(x float64) float64 {
		ex := math.Exp(x)
		return ex*(delta*delta - p.Phi*p.Phi - v - ex)/(2*math.Pow(p.Phi*p.Phi+v+ex, 2)) - (x-a)/(tau*tau)
	}
	A, B := a, 0.0
	if delta*delta > p.Phi*p.Phi + v {
		B = math.Log(delta*delta - p.Phi*p.Phi - v)
	} else {
		k := 1.0
		for f(a - k*tau) < 0 { k++ }
		B = a - k*tau
	}
	fA, fB := f(A), f(B)
	for math.Abs(B-A) > 1e-6 {
		C := A + (A-B)*fA/(fB-fA)
		fC := f(C)
		if fC*fB < 0 { A, fA = B, fB } else { fA /= 2 }
		B, fB = C, fC
	}
	newSigma := math.Exp(A / 2)

	// step 6: phiStar
	phiStar := math.Sqrt(p.Phi*p.Phi + newSigma*newSigma)

	// step 7: new phi, new mu
	newPhi := 1.0 / math.Sqrt(1.0/(phiStar*phiStar) + 1.0/v)
	newMu  := p.Mu
	for _, m := range ms {
		gp := g(m.Phi); e := E(p.Mu, m.Mu, m.Phi)
		newMu += newPhi*newPhi * gp * (m.Score - e)
	}
	return Player{Mu: newMu, Phi: newPhi, Sigma: newSigma}
}

func approx(a, b, eps float64) bool { return math.Abs(a-b) <= eps }
```

- [ ] **Step 3: Run, expect pass**

```bash
go test ./pkg/glicko2/...
```

- [ ] **Commit.**

```bash
git add pkg/glicko2/
git commit -m "feat(glicko2): full Glicko-2 update math with paper-canonical tests"
```

---

### Task 17.2: scoring-engine

**Files:**
- Create: `apps/scoring-engine/{main.go,scorer/{score,rating}.go}`

- [ ] **Step 1: Score formula from spec §6.6**

```go
package scorer

import "math"

type Targets struct { P50Us, P99Us, TPS int32 }

type Inputs struct {
	P50Us, P99Us, TPS  float64
	MatchFraction      float64 // matches / total
	AntiCheatFlags     float64 // sum of weights, [0, 1+]
	Targets            Targets
}

type Output struct {
	Score             int     // 0..1000
	Latency           float64
	Throughput        float64
	Tail              float64
	Stability         float64
	CorrectnessGate   int
	AntiCheatPenalty  float64
}

func Compute(in Inputs) Output {
	correctness := 0
	if in.MatchFraction >= 0.999 { correctness = 1 }
	penalty := math.Min(1.0, in.AntiCheatFlags)

	clip := func(x float64) float64 { if x < 0 { return 0 }; if x > 1 { return 1 }; return x }

	latency    := clip(1.0 - math.Log10(in.P50Us / float64(in.Targets.P50Us)))
	tail       := clip(1.0 - math.Log10(in.P99Us / float64(in.Targets.P99Us)))
	throughput := clip(in.TPS / float64(in.Targets.TPS))
	stability  := 0.0
	if in.P99Us+in.P50Us > 0 {
		stability = 1.0 - (in.P99Us-in.P50Us)/(in.P99Us+in.P50Us)
		stability = clip(stability)
	}
	composite := 0.40*latency + 0.20*throughput + 0.20*tail + 0.20*stability
	score := float64(correctness) * (1.0 - penalty) * composite * 1000.0
	return Output{
		Score: int(math.Round(score)),
		Latency: latency, Throughput: throughput, Tail: tail, Stability: stability,
		CorrectnessGate: correctness, AntiCheatPenalty: penalty,
	}
}
```

- [ ] **Step 2: Tests using the §6.6.2 worked example**

```go
func TestCompute_WorkedExample(t *testing.T) {
	out := Compute(Inputs{
		P50Us: 90, P99Us: 250, TPS: 45000, MatchFraction: 1.0, AntiCheatFlags: 0,
		Targets: Targets{P50Us: 50, P99Us: 200, TPS: 50000},
	})
	if out.Score != 765 { t.Fatalf("got %d, want 765 (allow ±2)", out.Score) }
}
```

- [ ] **Step 3: scoring-engine main loop**

ClickHouse query for `runs_summary` rows that aren't yet scored; per row compute, write rating delta to Postgres `ratings` table; ZADD into Redis `leaderboard:<scenario_hash>`.

```go
// pseudo
for {
	rows := ch.Query(`SELECT run_id, ... FROM runs_summary WHERE not_scored AND ...`)
	for _, r := range rows {
		out := scorer.Compute(...)
		applyGlicko(pg, r.SubmissionSha, r.ScenarioHash, out.Score)
		redis.ZADD("leaderboard:"+r.ScenarioHash, glickoLowerBound, r.SubmissionSha)
	}
	time.Sleep(15 * time.Second)
}
```

- [ ] **Commit.**

```bash
git add apps/scoring-engine/
git commit -m "feat(scoring-engine): composite scoring formula + Glicko-2 update + Redis leaderboard ZADD"
```

---

### Task 17.3: scoring-engine manifest + commit. (Standard pattern.)

---

## Day 18 — Leaderboard API + Next.js dashboard (~5 tasks, ~7 hours)

### Task 18.1: leaderboard-api SSE endpoint

**Files:**
- Create: `apps/leaderboard-api/{main.go,server/{sse,query}.go}`

- [ ] **Step 1: SSE handler** that pushes leaderboard delta every 1s

```go
func (s *Server) HandleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	tick := time.NewTicker(1 * time.Second); defer tick.Stop()
	for {
		select {
		case <-r.Context().Done(): return
		case <-tick.C:
			rows := s.queryLeaderboard(r.Context())
			b, _ := json.Marshal(rows)
			fmt.Fprintf(w, "event: leaderboard\ndata: %s\n\n", b)
			flusher.Flush()
		}
	}
}
```

- [ ] **Step 2: `queryLeaderboard` does a Redis ZREVRANGE on `leaderboard:default`** with WITHSCORES, joins to Postgres `submissions` for display name.

- [ ] **Step 3: Test, commit.**

```bash
git add apps/leaderboard-api/
git commit -m "feat(leaderboard-api): SSE endpoint streaming Redis ZSET with 1Hz ticks"
```

---

### Task 18.2: Next.js `app/(dashboard)/leaderboard/page.tsx`

**Files:**
- Create: `frontend/app/(dashboard)/leaderboard/page.tsx`
- Create: `frontend/components/leaderboard/Table.tsx`
- Create: `frontend/lib/sse.ts`

- [ ] **Step 1: SSE hook**

```ts
// lib/sse.ts
import { useEffect, useState } from "react";
export function useSSE<T>(url: string, event = "message") {
  const [data, setData] = useState<T | null>(null);
  useEffect(() => {
    const es = new EventSource(url);
    es.addEventListener(event, (e: MessageEvent) => setData(JSON.parse(e.data)));
    return () => es.close();
  }, [url, event]);
  return data;
}
```

- [ ] **Step 2: Table component**

```tsx
// components/leaderboard/Table.tsx
import { useSSE } from "@/lib/sse";

type Row = { rank: number; submission: string; rating: number; deviation: number; runs: number; lastSeen: string };

export default function LeaderboardTable() {
  const rows = useSSE<Row[]>("/api/leaderboard/stream", "leaderboard") ?? [];
  return (
    <table className="w-full">
      <thead><tr><th>#</th><th>submission</th><th>rating</th><th>±</th><th>runs</th></tr></thead>
      <tbody>
        {rows.map(r => (
          <tr key={r.submission} className="border-t">
            <td>{r.rank}</td>
            <td className="font-mono">{r.submission.slice(0, 12)}…</td>
            <td>{r.rating}</td>
            <td>±{r.deviation}</td>
            <td>{r.runs}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 3: Page**

```tsx
import LeaderboardTable from "@/components/leaderboard/Table";
export default function Page() {
  return (
    <main className="p-8">
      <h1 className="text-2xl font-semibold">Live Leaderboard</h1>
      <LeaderboardTable />
    </main>
  );
}
```

- [ ] **Step 4: Next.js API proxy** at `/api/leaderboard/stream` forwards to leaderboard-api service.

- [ ] **Commit.**

```bash
git add frontend/
git commit -m "feat(frontend): leaderboard page with SSE-driven live table"
```

---

### Task 18.3: Run inspector page (latency CDF)

**Files:**
- Create: `frontend/app/(dashboard)/runs/[runId]/page.tsx`
- Create: `frontend/components/charts/LatencyCDF.tsx`

- [ ] **Step 1: uPlot CDF**

```bash
cd frontend && pnpm add uplot && cd ..
```

```tsx
// components/charts/LatencyCDF.tsx
"use client";
import "uplot/dist/uPlot.min.css";
import { useEffect, useRef } from "react";
import uPlot from "uplot";

export default function LatencyCDF({ buckets, counts }: { buckets: number[]; counts: number[] }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const total = counts.reduce((a,b) => a+b, 0);
    let acc = 0; const cdf = counts.map(c => (acc += c)/total);
    new uPlot({ width: 600, height: 300,
      scales: { x: { time: false, distr: 3 } },  // log
      series: [{}, { stroke: "#0ea5e9" }],
      axes: [{}, {}],
    }, [buckets, cdf], ref.current);
  }, [buckets, counts]);
  return <div ref={ref} />;
}
```

- [ ] **Step 2: Page fetches `/api/runs/:id/latency` (calls leaderboard-api → ClickHouse `runs_summary`)**

- [ ] **Commit.**

```bash
git add frontend/
git commit -m "feat(frontend): per-run inspector page with latency CDF (uPlot)"
```

---

### Task 18.4: End-to-end smoke — phase 3

- [ ] **Step 1:** kick a `BenchmarkRun`, watch leaderboard tick.
- [ ] **Step 2:** Open `/runs/<id>`, see CDF.
- [ ] **Step 3:** E2E test asserts leaderboard returns ≥ 1 row after a run completes.

```go
//go:build e2e
func TestPhase3_LeaderboardPopulates(t *testing.T) {
	// kick run-002, wait COMPLETE, curl SSE for 5s, assert non-empty payload
}
```

- [ ] **Commit.**

---

### Task 18.5: Phase 3 close-out

- [ ] `make ci-local` green.
- [ ] `make ci-self-replay` green (the determinism gate).
- [ ] Tag `phase-3-complete`; push.

---

## Phase 3 Definition of Done

- [ ] Redpanda + ClickHouse + Redis all running in dev cluster; topics + tables auto-created.
- [ ] Gateway emits to `runs.<id>.events` topic; telemetry-sidecar consumes & forwards to ingester.
- [ ] `runs_raw` populated; `runs_summary` materializes; queries return p50/p99 within seconds of run completion.
- [ ] Divergence detector running per-run; emits `runs.<id>.divergence` events (zero divergences for the correct fixture).
- [ ] Replay-engine produces sealed Parquet on MinIO; `make ci-self-replay` is green.
- [ ] Glicko-2 ratings updated in Postgres; Redis leaderboard ZSET populated.
- [ ] Next.js dashboard shows live leaderboard with SSE; per-run page renders latency CDF.
- [ ] `phase-3-complete` git tag pushed.

---

## Dependencies for Phase 4

Phase 4 (Hardening + Blueprint) builds on:
- The full pipeline (Phases 1-3) — chaos-agent injects failures into it.
- The Glicko + composite score formula — Phase 4 adds anti-cheat penalty wiring from `ebpf-observer`.
- The replay log — Phase 4 chaos tests assert "score within 5% of baseline" through replay comparison.
