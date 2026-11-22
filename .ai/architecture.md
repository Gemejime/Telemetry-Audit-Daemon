# Architecture Context - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.23+ (64-bit) | High-throughput concurrent consumer & batcher, `GOMEMLIMIT=1800MiB`, `GOGC=80` |
| **Memory Discipline** | `sync.Pool` + `[]byte` Aho-Corasick | Amortized $\le 128\text{ B/op}$ redaction without `string` casting until persistence |
| **Streaming Ingress** | Apache Kafka 3.7+ | Topic `audit-compliance-records` (`min.insync.replicas=2`, `acks=all`, 7d retention, partitioned by `hash(tenant_id)`) |
| **Caller SDK** | `pkg/auditclient` (Go) | Dual-channel producer: fail-closed `ComplianceChannel` & droppable `RingBuffer` |
| **Hot Tier (Forensics)** | ClickHouse (`ReplicatedMergeTree`) | Native TCP RowBinary stream (`1,000 ms` or `2,500` records, P99 $\le 2\text{ s}$) |
| **Cold Tier (WORM Vault)** | AWS S3 + Object Lock (Compliance) | Micro-batched Parquet (`60 s` or `32 MiB`, P99 $\le 60\text{ s}$), 7-year retention, KMS CMK, CloudTrail |
| **Spillover Cache** | Local NVMe `mmap_wal` | Memory-mapped file buffer (`5 GiB` upper bound on `10 GiB` `emptyDir`) absorbing outages |
| **Admin & Ops HTTP** | `net/http` + OpenAPI 3.1 | `/healthz`, `/ready`, `/metrics`, `/v1/reconcile/gaps`, `/openapi.yaml`, `/docs` |
| **Containerization** | Distroless (`gcr.io/distroless/static:nonroot`) | Non-root UID `65532`, read-only rootfs, zero shell utilities |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with `tasks/Production Grade Backend Concepts.pdf`:

### Layer 1: API & Routing
- **Entry Points (`cmd/audit-daemon/main.go`, `cmd/ledger-verify/main.go`)**:
  - `audit-daemon`: Initializes 12-factor configuration, `sync.Pool` byte pools, Aho-Corasick redaction engine, ClickHouse TCP pool, S3 WORM uploader, `mmap_wal` spillover, Kafka consumer group, and HTTP admin/probe server. Handles `SIGINT`/`SIGTERM` by halting Kafka consumption, flushing in-flight ClickHouse/S3/WAL batches, and committing offsets before exit.
  - `ledger-verify`: Standalone CLI scanning S3 Parquet metadata for `[min_seq, max_seq]` sequence gaps and invoking `reconcile_partition_gaps`.
- **Endpoints & Routing (`internal/infrastructure/transport/http/`)**:
  - `GET /healthz`, `GET /ready` (gated on Kafka consumer group health & drain state), `GET /metrics` (Prometheus), `POST /v1/reconcile/gaps` (admin reconciliation), `GET /openapi.yaml`, `GET /docs`.
- **Middleware**:
  - Request/trace ID correlation, bearer auth validation on `/v1/reconcile/gaps`, Prometheus HTTP latency counters, and panic recovery.
- **Input Validation & Sanitization**:
  - Strict validation of inbound Kafka `AuditRecord` protobuf payloads (`tenant_id`, `seq_id`, `event_time`, `actor_context`), HTTP reconciliation parameters, and `pkg/redaction` PII/PHI masking before any storage write.
- **Rate Limiting & Throttling (Backpressure)**:
  - Caller-side bounded channels in `pkg/auditclient`: read-plane sheds non-essential metadata at $90\%$ fill; write-plane blocks and returns a typed sentinel error (`ErrAuditUnavailable`) which HTTP caller services map to `HTTP 503`.
  - Admin HTTP server rate-limits `POST /v1/reconcile/gaps` via token-bucket middleware to prevent ClickHouse/S3 scan amplification.
- **Idempotency**:
  - Partition-scoped monotonic `(producer_id, tenant_id, partition, seq_id)` ordering and `ReplicatedReplacingMergeTree` in ClickHouse prevent duplicate or out-of-order hot-tier ledger rows on consumer rebalances or 60s S3-hold replays. In the cold S3 WORM tier, immutable Parquet objects (`s3://vault/tenant_<id>/<hash>/year=YYYY/.../min_seq-max_seq.parquet`) may span overlapping `[min_seq, max_seq]` ranges across replays; cold-tier readers and reconciliation must deduplicate overlapping intervals and emit an overlap alert when overlaps are detected.

### Layer 2: Data Persistence & State
- **Database Migrations**:
  - Versioned ClickHouse DDL schemas (`ReplicatedReplacingMergeTree` partitioned by `toYYYYMM(event_time)`, ordered by `(tenant_id, event_time, producer_id, seq_id)`).
- **Atomic Batch Commits (Zero Dual-Write Loss)**:
  - Kafka consumer offsets are committed **only after** ClickHouse confirms RowBinary writes and S3 confirms WORM Parquet `PutObject`. During declared downstream outages (ClickHouse/S3), offset advancement halts and records spool to pod-local `mmap_wal` (`5 GiB` cap) for bounded retry without heap growth.
- **Connection Pooling**:
  - ClickHouse native TCP connection pool (`internal/infrastructure/persistence/clickhouse/client.go`) and reused HTTP/2 S3 client transport.
- **Spillover & Batch Bounds**:
  - Hot batch limit: `1,000 ms` or `2,500` records. Cold batch limit: `60 s` or `32 MiB`. Spillover cap: `5 GiB` NVMe `mmap_wal`.
- **Resource Identification**:
  - High-entropy ULIDs/UUIDs for `event_id` and `trace_id`; monotonic `uint64 seq_id` per `(producer_id, tenant_id, partition)` correlated with Kafka `partition_offset`.

### Layer 3: Architecture & Scale
- **Asynchronous Task Queues & Workers**:
  - Stateless Kafka consumer group (`internal/infrastructure/kafka/consumer_group.go`) processing `audit-compliance-records` partitions in parallel.
- **Concurrency Control**:
  - Per-partition sequential batching (`[min_seq, max_seq]`) eliminates cross-goroutine lock contention and dual-write race conditions.
- **High Availability & Redundancy**:
  - Stateless Kubernetes Deployment (`3–8` replicas, HPA targeting Kafka consumer lag and `70%` CPU), Kafka `min.insync.replicas=2`, ClickHouse `ReplicatedMergeTree`, and S3 multi-AZ WORM storage.
- **Creational Design Patterns**:
  - Explicit constructor-based dependency injection in `cmd/audit-daemon/main.go` wiring domain entities, repository ports (`HotStorePort`, `ColdStorePort`, `WALPort`), application commands/queries, and infrastructure adapters.

### Layer 4: Security & Identity
- **AuthN vs AuthZ**:
  - Tenant isolation requires validating each producer's trusted tenant binding (e.g., authenticated service/credential claim) before accepting its supplied `tenant_id`, independently of `hash(tenant_id)` Kafka partitioning and tenant-prefixed S3/ClickHouse storage key routing. All reads and writes of stored records—including access via admin and reconciliation endpoints (`/v1/reconcile/gaps`)—must enforce tenant-scoped authorization alongside scoped bearer auth; S3 writes use IAM least-privilege roles with KMS CMK and Object Lock Compliance Mode.
- **Configuration & Secrets Management**:
  - 12-factor environment variables; Kafka SASL/TLS, ClickHouse, and AWS credentials injected via Kubernetes Secrets / external secret stores (never committed to git).
- **Data Protection & Secured Logging**:
  - TLS/mTLS in transit; AWS KMS SSE-KMS + Object Lock at rest; Aho-Corasick/regex PII/PHI redaction over `[]byte`; structured JSON `log/slog` with strict credential and payload redaction.

### Layer 5: Observability & Quality
- **Structured Logging**: JSON logs via `log/slog` with `trace_id`, `tenant_id`, `partition`, `min_seq`, `max_seq`, and `duration_ms`.
- **Primary Prometheus Telemetry Metrics**:
  - `audit_records_consumed_total` (Counter, label: `tenant_id`)
  - `audit_consumer_lag_records` (Gauge)
  - `audit_hot_persist_duration_seconds` (Histogram, P99 $\le 1.5\text{ s}$)
  - `audit_cold_flush_duration_seconds` (Histogram)
  - `audit_caller_dropped_records_total` (Counter)
  - `audit_wal_spillover_active` (Gauge: `0` or `1`)
  - `audit_s3_upload_errors_total` (Counter, label: `status`)
- **Production Alert Rules**:
  - *High Consumer Lag (Warning)*: `audit_consumer_lag_records > 15000` for `>3m`.
  - *Hot Persist SLO Breach (Page)*: P99 `audit_hot_persist_duration_seconds > 2.0` for `>2m`.
  - *Active Disk Spillover (Warning)*: `audit_wal_spillover_active == 1` for `>5m`.
  - *Missing Sequence Gap (Page)*: Any sequence gap detected between contiguous batch manifests for an active tenant partition.
- **Health Checks & Graceful Error Handling**:
  - `/healthz` (liveness) and `/ready` (readiness gated on Kafka consumer group health & drain state); sanitized error responses.

---

## Hexagonal Folder Structure

```text
rag-telemetry-audit-daemon/
├── cmd/
│   ├── audit-daemon/
│   │   └── main.go                         # Kafka consumer bootstrap, lifecycle, signal traps
│   └── ledger-verify/
│       └── main.go                         # CLI tool scanning S3 Parquet metadata for sequence gaps
├── api/
│   ├── openapi/
│   │   └── openapi.yaml                    # OpenAPI 3.1 HTTP admin/probe/reconcile contract
│   └── proto/v1/
│       └── audit_record.proto              # Compliance record, access metadata, actor context
├── pkg/
│   ├── auditclient/                        # Shared Go SDK imported by caller services
│   │   ├── client.go                       # Dual-channel producer (Compliance vs. Diagnostic)
│   │   ├── ring_buffer.go                  # Diagnostic droppable ring buffer
│   │   └── compliance_channel.go           # Fail-closed bounded channel with backpressure
│   ├── pool/
│   │   └── byte_pool.go                    # sync.Pool reusable memory buffers
│   └── redaction/
│       ├── engine.go                       # Amortized PII/PHI masking engine over []byte
│       └── patterns.go                     # Standardized compliance token patterns
├── internal/
│   ├── domain/
│   │   ├── entity/
│   │   │   ├── compliance_entry.go         # Domain model for compliance audits
│   │   │   └── batch_manifest.go           # Tracks partition sequence intervals [min_seq, max_seq]
│   │   └── repository/
│   │       ├── hot_store_port.go           # ClickHouse interface
│   │       ├── cold_store_port.go          # S3 WORM interface
│   │       └── wal_port.go                 # Local spillover disk interface
│   ├── application/
│   │   ├── command/
│   │   │   ├── process_kafka_batch.go      # Validates, redacts, and dispatches batch
│   │   │   ├── flush_clickhouse.go         # RowBinary stream committer
│   │   │   └── flush_s3_worm.go            # Parquet compilation and WORM persistence
│   │   └── query/
│   │       └── reconcile_partition_gaps.go # Scans ClickHouse & S3 for dropped sequences
│   └── infrastructure/
│       ├── kafka/
│       │   └── consumer_group.go           # High-throughput partitioned consumer
│       ├── persistence/
│       │   ├── clickhouse/
│       │   │   ├── client.go               # Native TCP connection pool
│       │   │   └── row_binary_writer.go    # Bulk stream writer
│       │   ├── s3/
│       │   │   ├── parquet_writer.go       # Columnar Parquet builder
│       │   │   └── worm_uploader.go        # PutObject with Object Lock Retention Headers
│       │   └── disk/
│       │       └── mmap_wal.go             # Local memory-mapped spillover file buffer (5 GiB cap)
│       └── transport/
│           └── http/
│               ├── server.go               # Admin, reconciliation, metrics, & OpenAPI server
│               └── health_handler.go       # Readiness gated on Kafka consumer health
├── deployments/
│   ├── helm/                               # Kubernetes Deployment (3–8 replicas, HPA, emptyDir 10Gi)
│   └── docker/
│       └── Dockerfile                      # Distroless static non-root build
├── test/
│   ├── unit/                               # Redaction efficiency and sequence tracking tests
│   ├── integration/                        # Testcontainers suites (Kafka + ClickHouse + LocalStack)
│   ├── performance/                        # 35k RPS throughput and memory allocation tests
│   └── chaos/                              # S3 partition failure and broker drop tests
├── .spectral.yaml
├── Makefile
├── go.mod
└── go.sum
```
