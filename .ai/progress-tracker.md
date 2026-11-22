# Progress Tracker & Implementation Roadmap - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

## 1. Calibrated Implementation Phases

This roadmap tracks the implementation milestones detailed in `tasks/RAG Telemetry Audit Daemon Plan.pdf`:

| Phase | Focus Area | Deliverables & Exit Gates | Status |
| :--- | :--- | :--- | :--- |
| **Phase 0: Repository & AI Spec Alignment** | Governance, CI/CD & Contracts | - `.gitignore` excluding `tasks/`, `.spectral.yaml`, and GitHub Actions CI (`lint-and-typecheck`, `security-and-logging-audit`, `OpenAPI 3.1 & Doc API Contract Sync`, `test-suites`).<br>- Updated `.ai.md`, `.ai/*`, and `GEMINI.MD` aligned with `rag-telemetry-audit-daemon` and `Production Grade Backend Concepts.pdf`, plus pressure-test contract hardening (`producer_id` seq scope, ClickHouse `ReplicatedReplacingMergeTree` DDL, Parquet schema). | Completed |
| **Phase 1: Ingress Contracts & Client SDK** (Weeks 1–2) | Decoupled Ingress Architecture | - `go.mod`, `Makefile`, `buf.yaml`/`buf.gen.yaml`, `api/proto/v1/audit_record.proto` + generated `audit_record.pb.go`, and `api/openapi/openapi.yaml` (Spectral-validated).<br>- `pkg/auditclient/` (`client.go` with `franz-go` `acks=all` & `hash(tenant_id)` FNV-1a partitioner, `compliance_channel.go` isolated write-plane fail-closed `ErrAuditUnavailable` & read-plane 90% metadata shedding, `ring_buffer.go` droppable diagnostic buffer).<br>- Kafka topic manifest `deployments/kafka/audit-compliance-records.yaml` (`partitions: 8`, `replicas: 3`, `min.insync.replicas: "2"`, 7d retention). | Completed |
| **Phase 2: Redaction Engine & Storage Adapters** (Weeks 3–4) | Data Sanitization & ClickHouse Sink | - `pkg/pool/byte_pool.go` (`sync.Pool` reusable `[]byte` buffers) & `pkg/redaction/` (`engine.go`, `patterns.go` Aho-Corasick/regex PII masking $\le 128\text{ B/op}$).<br>- `internal/domain/` (`compliance_entry.go`, `batch_manifest.go`, repository ports).<br>- ClickHouse RowBinary ingestion adapter (`internal/infrastructure/persistence/clickhouse/`) with automatic retries.<br>- Local NVMe `mmap_wal.go` spillover (`5 GiB` cap) to absorb downstream ClickHouse/S3 outages. | Pending |
| **Phase 3: S3 WORM & Metadata Indexing** (Weeks 5–6) | Cold Compliance Persistence | - Micro-batched Parquet encoder (`parquet_writer.go`) utilizing `60 s` / `32 MiB` flush boundaries.<br>- S3 client integration (`worm_uploader.go`) using Object Lock in Compliance Mode (7-year retention, KMS CMK) with `[min_seq, max_seq]` Parquet metadata.<br>- `reconcile_partition_gaps.go` & `cmd/ledger-verify/main.go` verifying cross-tier ClickHouse $\leftrightarrow$ S3 consistency.<br>- HTTP admin/probe server (`internal/infrastructure/transport/http/`). | Pending |
| **Phase 4: Chaos Testing & Production Rollout** (Weeks 7–8) | Hardening & Scale Validation | - Integration suites (`test/integration/` with Kafka, ClickHouse, LocalStack S3 Object Lock).<br>- Chaos tests (`test/chaos/`): S3 `503 SlowDown`/outages, ClickHouse failovers, Kafka broker restarts & rebalances.<br>- Performance validation (`test/performance/`) at `35,000 events/sec` confirming RSS $\le 1.5\text{ GiB}$.<br>- `deployments/docker/Dockerfile` (distroless non-root UID `65532`) & `deployments/helm/` canary rollout (`3–8` pods, `1000m/2000m` CPU, `1Gi/2Gi` RAM, `GOMEMLIMIT=1800MiB`, `GOGC=80`, `10Gi` `emptyDir`). | Pending |

---

## 2. Architecture Decision Records (ADRs)

- **ADR-001: Cloud-Native WORM over Application Merkle Trees**: Deprecates application-level Merkle root hashing. Adopts AWS S3 Object Lock in Compliance Mode along with KMS CMK auditing and CloudTrail data events to fulfill SOC 2 Type II and HIPAA tamper-resistance requirements.
- **ADR-002: Dual Ingestion Routing Architecture**: Diagnostic performance traces are routed directly to an OpenTelemetry Collector (`rag-trace-collector-worker`). Compliance-critical audit logs route exclusively to Kafka (`audit-compliance-records`), isolating system monitoring overhead from legal compliance paths.
- **ADR-003: Two-Tier Storage Flush Intervals**: ClickHouse provides hot analytical indexing within a `2-second` P99 SLO (`1,000 ms` / `2,500` records); S3 cold archival uses a `60-second` / `32 MiB` batch flush threshold (not a guaranteed archival time, as actual archival completion depends on S3 availability, with `mmap_wal` buffering outages), preventing S3 API saturation and excessive `PUT` costs.
- **ADR-004: Standardized Kubernetes Deployment Topology**: Replaces the mixed DaemonSet and Deployment setup with a standard horizontal Kubernetes Deployment (`3–8` replicas), scaling pods based on Kafka topic partition consumer lag.

---

## 3. Operational Runbooks

### Runbook 1: Kafka Consumer Lag Spike
- **Symptom**: Prometheus alert `audit_consumer_lag_records > 10000` (or `> 15000` for `>3m`) fires.
- **Remediation**: Check ClickHouse write latency metrics (`audit_hot_persist_duration_seconds`). If ClickHouse is healthy, scale the Kubernetes Deployment up (up to `8` replicas / matching the total partition count of `audit-compliance-records`).

### Runbook 2: S3 Rate Limiting (`503 SlowDown`) Mitigation
- **Symptom**: `audit_s3_upload_errors_total{status="503"}` increments.
- **Remediation**: Verify S3 key structure. Ensure files include a randomized prefix or hash segment (`s3://vault/tenant_<id>/<hash>/year=YYYY/...`) to spread write operations evenly across AWS S3 storage partitions.

### Runbook 3: Missing Sequence Reconciliation
- **Symptom**: CLI tool `ledger-verify` (or `POST /v1/reconcile/gaps`) reports a gap in `[min_seq, max_seq]` for a tenant; record the affected `producer_id` alongside `tenant_id` and `partition` to distinguish overlapping sequence ranges from different producers.
- **Remediation**: Run `reconcile_partition_gaps` against ClickHouse using `(tenant_id, partition, producer_id)` to check if records exist in the hot tier but were delayed in S3 Parquet batch compilation. If data is missing in both ClickHouse and S3, check Kafka `audit-compliance-records` retention for the affected partition and replay the missing records if still within retention; if retention has expired and replay is unavailable, escalate immediately per compliance incident procedure. (Separately, `audit_caller_dropped_records_total` may be inspected for diagnostic context on upstream shedding or diagnostic-channel drops, not as a recovery step.)

---

## 4. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
