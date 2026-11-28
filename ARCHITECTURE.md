# Architecture — `rag-telemetry-audit-daemon`

## 1. System Overview & Dual-Plane Isolation

`rag-telemetry-audit-daemon` is the compliance ledger engine of the disaggregated RAG platform. It structurally isolates **compliance-critical access and mutation records** from high-cardinality diagnostic telemetry:

```text
Upstream Services (rag-search-orchestrator / Edge Gateway / rag-kb-catalog-service)
│
├── [Compliance Plane: pkg/auditclient.ComplianceChannel]
│   ├── Write-plane mutations: synchronous acks=all (fail-closed -> HTTP 503 if unavailable)
│   └── Read-plane queries: non-blocking bounded buffer (90% metadata shedding under pressure)
│         │
│         ▼
│   Apache Kafka ("audit-compliance-records", partitioned by FNV-1a hash(tenant_id))
│         │
│         ▼
│   rag-telemetry-audit-daemon (Stateless Kubernetes Deployment, 3–8 replicas)
│   ├── pkg/redaction + pkg/pool     (in-flight []byte PII/PHI masking, <= 128 B/op)
│   ├── entity.SequenceTracker       (monotonic [min_seq, max_seq] per producer/tenant/partition)
│   ├── internal/application/command (ProcessKafkaBatch, HotFlusher, ColdFlusher)
│   │     ├──► ClickHouse (Native TCP RowBinary, 1,000 ms / 2,500 records, P99 <= 2 s)
│   │     ├──► AWS S3 WORM (Parquet, 60 s / 32 MiB, Object Lock Compliance Mode, 7y)
│   │     └──► NVMe mmap_wal (5 GiB spillover cap on downstream outage; halts Kafka commits)
│   └── internal/infrastructure/transport/http
│         └── /healthz, /ready, /metrics, POST /v1/reconcile/gaps, /openapi.yaml, /docs
│
└── [Diagnostic Plane: pkg/auditclient.RingBuffer]
    └── Droppable ring buffer -> OpenTelemetry Collector -> rag-trace-collector-worker
```

---

## 2. Calibrated SLIs & SLOs

| Metric | Target | Enforcement Mechanism |
| :--- | :--- | :--- |
| **Ingress Throughput** | >= 35,000 events/sec | Partitioned `franz-go` consumer group, zero-copy `[]byte` processing |
| **ClickHouse Hot Lag** | P99 <= 2 s | RowBinary stream flushed every `1,000 ms` or `2,500` records |
| **S3 WORM Cold Lag** | P99 <= 60 s | Micro-batched Parquet flushed every `60 s` or `32 MiB` |
| **Redaction Allocation** | <= 128 B/op | `sync.Pool` byte slices (`pkg/pool/byte_pool.go`) |
| **Pod Memory & Spillover** | RSS <= 1.5 GiB | `GOMEMLIMIT=1800MiB`, `GOGC=80`, `1Gi/2Gi` pod, `5 GiB` `mmap_wal` cap |

---

## 3. Hexagonal Architecture Layers

### Domain Layer (`internal/domain/`)
- **`entity/compliance_entry.go`**: Core compliance record model.
- **`entity/batch_manifest.go`**: `SequenceTracker` tracking contiguous `[min_seq, max_seq]` ranges per `(producer_id, tenant_id, kafka_partition)` and detecting gaps.
- **`repository/`**: Port interfaces (`HotStorePort`, `ColdStorePort`, `WALPort`) decoupling business logic from ClickHouse, S3, and disk implementations.

### Application Layer (`internal/application/`)
- **`command/process_kafka_batch.go`**: Decodes Protobuf `AuditRecord` frames, runs `pkg/redaction` over pooled `[]byte`, updates sequence manifests, dispatches to hot/cold flushers, and spills to `mmap_wal` on downstream failure.
- **`command/flush_clickhouse.go` & `command/flush_s3_worm.go`**: Bounded batch flushers enforcing time and size/count thresholds.
- **`query/reconcile_partition_gaps.go`**: Cross-tier reconciliation comparing ClickHouse sequence ranges against S3 Parquet object metadata.

### Infrastructure Layer (`internal/infrastructure/`)
- **`config/dotenv.go`**: Startup `.env` loader for local/integration execution without overriding exported process environment variables.
- **`kafka/consumer_group.go`**: Stateless `franz-go` consumer group that commits offsets **only after** both ClickHouse and S3 WORM writes succeed (or pauses offset advancement during WAL spillover).
- **`persistence/clickhouse/`**: Native TCP connection pool (`client.go`) and RowBinary writer (`row_binary_writer.go`) with bounded exponential backoff.
- **`persistence/s3/`**: Columnar Parquet builder (`parquet_writer.go`) and HTTP/2 S3 uploader (`worm_uploader.go`) setting Object Lock Compliance Mode retention headers, KMS CMK encryption, and `[min_seq, max_seq]` metadata.
- **`persistence/disk/`**: Memory-mapped NVMe WAL (`mmap_wal.go`) with Linux `mmap` and portable fallback implementations.
- **`transport/http/`**: Health/readiness probes (`/healthz`, `/ready`), Prometheus `/metrics`, JWT/static-token authenticated & rate-limited `POST /v1/reconcile/gaps`, `/openapi.yaml`, and `/docs`.
- **`observability/metrics.go`**: Prometheus metrics (`audit_records_consumed_total`, `audit_consumer_lag_records`, `audit_hot_persist_duration_seconds`, `audit_cold_flush_duration_seconds`, `audit_wal_spillover_active`, `audit_s3_upload_errors_total`, etc.) with optional tenant label allowlisting (`METRICS_TENANT_ALLOWLIST`).

---

## 4. Configuration, Secrets & Deployment Topology

- **Container**: Distroless static non-root image (`deployments/docker/Dockerfile`, UID/GID `65532`, read-only root filesystem, `/var/lib/audit-daemon/wal` mount).
- **Kubernetes / Helm (`deployments/helm/rag-telemetry-audit-daemon/`)**:
  - `3–8` replicas via HPA (`autoscaling`), `10 GiB` `emptyDir` for `mmap_wal`, `NetworkPolicy` restricting ingress/egress to Prometheus, ops-admin, Kafka, ClickHouse, and S3.
  - Non-sensitive environment variables are synced from `.env.prod` into `values.yaml` (`scripts/sync_env_to_deployments.py`), rendered into `templates/configmap.yaml`, and hashed via `checksum/config` in `templates/deployment.yaml` to trigger rolling restarts on config changes.
  - Sensitive credentials (`CLICKHOUSE_PASSWORD`, `AUTH_JWT_SECRET`, optional `STATIC_ADMIN_TOKEN`) are injected into `rag-telemetry-audit-daemon-secrets` by `.github/workflows/deploy.yml` (see [deployments/SECRET_MANAGEMENT.md](deployments/SECRET_MANAGEMENT.md)).

---

## 5. Security & Immutability Invariants

- **Cloud-Native WORM**: Uses AWS S3 Object Lock (Compliance Mode, 7-year retention), KMS CMK, and CloudTrail data events rather than application-level Merkle trees.
- **Secured Logging**: All runtime logs use structured JSON (`log/slog`) and redact `Authorization` headers, Bearer tokens, ClickHouse/Kafka/AWS credentials, and raw PII/PHI payloads.
