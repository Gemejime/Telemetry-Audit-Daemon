# Phase 3 Implementation Plan: S3 WORM, Metadata Indexing & Daemon Orchestration

**Status**: Completed  
**Spec References**:
- [`GEMINI.md`](../GEMINI.md)
- `tasks/RAG Telemetry Audit Daemon Plan.pdf`
- [`.ai/progress-tracker.md`](../.ai/progress-tracker.md)
- [`.ai/architecture.md`](../.ai/architecture.md)
- [`.ai/doc-api.md`](../.ai/doc-api.md)
- [`.ai/code-standards.md`](../.ai/code-standards.md)
- [`.ai/security.md`](../.ai/security.md)
- [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md)

---

## 1. Goals & Exit Criteria

Implement **Phase 3: S3 WORM & Metadata Indexing (Weeks 5–6)** from [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), plus the daemon/application orchestration needed to wire Phase 1–3 components end-to-end:

1. **Micro-Batched Parquet Encoder (`internal/infrastructure/persistence/s3/parquet_writer.go`)**:
   - Uses `github.com/parquet-go/parquet-go` (pure Go, zero CGO) to encode `[]entity.ComplianceEntry` into columnar Parquet buffers matching the exact schema in [`.ai/doc-api.md`](../.ai/doc-api.md):
     - Columns: `event_id`, `trace_id`, `tenant_id`, `producer_id`, `kafka_partition` (`INT32`), `kafka_offset` (`INT64`), `seq_id` (`INT(64, false)`), `event_time_ms` (`INT64/TIMESTAMP_MILLIS`), `plane` (`INT32`), `action`, `service_name`, `actor_id`, `auth_method`, `source_ip_hash`, `doc_ids` (`LIST<INT(32, false)>`), `chunk_ids` (`LIST<BYTE_ARRAY>`), `document_versions` (`LIST<BYTE_ARRAY>`), `cache_hit` (`BOOLEAN`), `embedding_model_version`, `payload_redacted` (`BYTE_ARRAY`), `prompt_tokens` (`INT(32, false)`), `completion_tokens` (`INT(32, false)`).
     - File Key-Value Metadata: `tenant_id`, `producer_id`, `kafka_partition`, `min_seq`, `max_seq`, `record_count`, `schema_version`.
   - Also provides a Parquet metadata reader helper (`ReadParquetManifest`) using `parquet.OpenFile` over an `io.ReaderAt` backed by S3 `Range` requests (reading only the Parquet footer/tail bytes, never full 32 MiB objects) to extract `[min_seq, max_seq]` and `producer_id` key-value metadata for reconciliation.

2. **S3 WORM Client Integration (`internal/infrastructure/persistence/s3/worm_uploader.go`)**:
   - Uses `github.com/aws/aws-sdk-go-v2/service/s3` with HTTP/2 connection reuse and configurable endpoint (for AWS S3 & LocalStack integration testing).
   - Implements `repository.ColdStorePort`:
     - Object key format with unique per-upload randomized/hashed prefix to prevent `503 SlowDown` and avoid key collisions across different `producer_id`s or replayed `[min_seq, max_seq]` ranges under Object Lock Compliance Mode: `tenant_<tenant_id>/<hash>/year=YYYY/month=MM/day=DD/p<partition>_<min_seq>_<max_seq>.parquet`.
     - Object Lock Compliance Mode (`ObjectLockModeCompliance`), 7-year retention (`now + 7*365*24h`), `ServerSideEncryptionAwsKms`, `SSEKMSKeyId` (from 12-factor env), and `BucketKeyEnabled: true`.
     - Exponential backoff retries on `503 SlowDown` / `500`, incrementing `audit_s3_upload_errors_total{status=...}` and `audit_cold_flush_duration_seconds`.
     - Lists S3 objects and reads Parquet key-value metadata via bounded-concurrency footer `Range` requests for `reconcile_partition_gaps.go` and `cmd/ledger-verify`.

3. **Domain Port Extensions & Application Layer (`internal/domain/repository/`, `internal/application/`)**:
   - Extend `HotStorePort` / `ColdStorePort` to query contiguous sequence intervals `[min_seq, max_seq]` per `(tenant_id, partition, producer_id)` in `[from_seq, to_seq]`:
     - ClickHouse (`row_binary_writer.go` via a dedicated `QueryConn` interface so existing `BatchConn` mocks remain intact) queries `FINAL`-deduplicated `seq_id`s / contiguous gap-and-island runs (not just `MIN`/`MAX`, which would hide internal gaps).
     - S3 `ListManifests` returns Parquet footer manifests `[min_seq, max_seq]` per `producer_id`.
   - `internal/application/command/process_kafka_batch.go`:
     - Decodes `auditv1.AuditRecord` protobufs, validates required fields via `ComplianceEntry.Validate()`, redacts `PayloadRedacted` in-flight via `pkg/redaction.Engine` ($\le 128\text{ B/op}$), and tracks sequence continuity via `entity.SequenceTracker`.
     - **Poison-pill & tenant-mismatch isolation**: Before records reach either flusher, each producer's trusted tenant is resolved from an authenticated source (such as the Kafka mTLS/SASL authenticated producer principal or a configured `producer_id` $\rightarrow$ `tenant_id` binding registry) and compared against the payload `tenant_id` alongside `ComplianceEntry.Validate()`. Permanent protobuf unmarshal errors, `Validate()` errors, or producer-to-tenant mismatches are logged using redacted metadata only (`trace_id`, `partition`, `offset`, error), counted via `audit_malformed_records_total`, and skipped rather than wedging the partition.
     - Routes valid entries to hot/cold flushers, spilling to `mmap_wal` on downstream failures, and runs a background WAL drain/recovery loop (`WALPort.Drain` + `WALPort.Ack`) that replays spooled records to ClickHouse/S3 once storage stabilizes and updates `audit_wal_spillover_active`.
   - `internal/application/command/flush_clickhouse.go`: Buffers and flushes ClickHouse RowBinary batches every `1,000 ms` or `2,500` records (P99 $\le 2\text{ s}$), reporting flushed `(partition, offset)` watermarks.
   - `internal/application/command/flush_s3_worm.go`: Buffers and flushes S3 WORM Parquet batches every `60 s` or `32 MiB` **global** buffer cap (P99 $\le 60\text{ s}$), splitting per `(tenant_id, kafka_partition, producer_id)` stream on flush with bounded concurrent `PutObject` workers so `1.5 GiB` RSS / `GOMEMLIMIT=1800MiB` is never breached and tiny-file `PutObject` storms are avoided. Supports explicit partition flush on rebalance/shutdown.
   - `internal/application/query/reconcile_partition_gaps.go`: Cross-tier reconciliation comparing ClickHouse (`FINAL` contiguous runs) and S3 WORM Parquet `[min_seq, max_seq]` intervals across `[from_seq, to_seq]`, reporting `missing_in` (`"s3"`, `"clickhouse"`, `"both"`) per `api/openapi/openapi.yaml`, and flagging overlapping S3 intervals via `audit_s3_interval_overlaps_total`, structured `log/slog` warnings, and `cmd/ledger-verify` output.

4. **Kafka Consumer Group & Entry Points (`internal/infrastructure/kafka/consumer_group.go`, `cmd/audit-daemon/`, `cmd/ledger-verify/`)**:
   - `consumer_group.go`: `franz-go` consumer group subscribed to `audit-compliance-records` with **per-partition uncommitted-offset watermark tracking** across both ClickHouse (`1 s`) and S3 WORM (`60 s`), committing offsets **only up to** the lowest offset persisted by both tiers.
   - Handles `OnPartitionsRevoked` / `OnPartitionsLost` by force-flushing (or spilling to `mmap_wal`) in-flight records for revoked partitions and committing safe offsets before returning.
   - On `repository.ErrWALFull` (`5 GiB` cap), pauses affected partitions via `PauseFetchPartitions` (resuming once WAL space drains) instead of blocking the poll loop and triggering `max.poll.interval.ms` rebalance storms.
   - `cmd/audit-daemon/main.go`: 12-factor config, explicit DI wiring, `SIGINT`/`SIGTERM` graceful drain & offset commit.
   - `cmd/ledger-verify/main.go`: Standalone CLI scanning S3 Parquet metadata and invoking `reconcile_partition_gaps`.

5. **HTTP Admin, Probe & Metrics Server (`internal/infrastructure/transport/http/`)**:
   - `server.go` & `health_handler.go`: `GET /healthz`, `GET /ready`, `GET /metrics` (on main or dedicated `METRICS_ADDR` port per `.ai/security.md`), `POST /v1/reconcile/gaps` (bearer auth `audit:reconcile` scope + `tenant_id` claim authorization + token-bucket rate limiter + `X-Request-ID` + base-10 `uint64` string parsing for `from_seq`/`to_seq` per `api/openapi/openapi.yaml`), `GET /openapi.yaml`, `GET /docs`.
   - Prometheus metrics (`prometheus/client_golang`) matching `.ai/architecture.md` (plus `audit_s3_interval_overlaps_total` and `audit_malformed_records_total`).

---

## 2. Planned Files

| File | Purpose |
| :--- | :--- |
| `internal/infrastructure/observability/metrics.go` | Prometheus metrics registry & collectors |
| `internal/infrastructure/persistence/s3/parquet_writer.go` | Columnar Parquet encoder & key-value metadata reader |
| `internal/infrastructure/persistence/s3/worm_uploader.go` | AWS S3 SDK v2 Object Lock (Compliance, 7y, KMS CMK) uploader & lister |
| `internal/infrastructure/persistence/s3/s3_test.go` | Parquet schema/metadata round-trip & mock S3 Object Lock/503 tests |
| `internal/domain/repository/hot_store_port.go` | Add `QuerySequenceRanges` for reconciliation (ClickHouse `FINAL`) |
| `internal/domain/repository/cold_store_port.go` | Add `ListManifests` for S3 Parquet metadata reconciliation |
| `internal/infrastructure/persistence/clickhouse/row_binary_writer.go` | Implement `QuerySequenceRanges` with `FINAL` deduplication |
| `internal/application/command/flush_clickhouse.go` | `1,000 ms` / `2,500`-record ClickHouse flusher |
| `internal/application/command/flush_s3_worm.go` | `60 s` / `32 MiB` S3 WORM Parquet flusher |
| `internal/application/command/process_kafka_batch.go` | Protobuf decode, `pkg/redaction`, `SequenceTracker`, WAL spillover |
| `internal/application/query/reconcile_partition_gaps.go` | Cross-tier ClickHouse $\leftrightarrow$ S3 `[min_seq, max_seq]` gap & overlap detector |
| `internal/infrastructure/kafka/consumer_group.go` | `franz-go` partitioned consumer with commit-after-persist |
| `internal/infrastructure/transport/http/health_handler.go` | `/healthz` and `/ready` handlers |
| `internal/infrastructure/transport/http/server.go` | Admin/reconcile/OpenAPI/metrics HTTP server with auth & rate limit |
| `cmd/audit-daemon/main.go` | Daemon bootstrap, DI wiring, graceful `SIGINT`/`SIGTERM` shutdown |
| `cmd/ledger-verify/main.go` | Standalone CLI for S3 Parquet gap verification & reconciliation |

---

## 3. Architecture Decisions for Human-in-the-Loop Confirmation

1. **Parquet & S3 Libraries**: Use `github.com/parquet-go/parquet-go` (pure Go) for Parquet encoding/metadata reading and `github.com/aws/aws-sdk-go-v2` for S3 Object Lock uploads.
2. **Prometheus Metrics Library**: Use `github.com/prometheus/client_golang` for `/metrics` exposition and metric recording.
3. **HTTP Auth & Port Isolation**: Validate `POST /v1/reconcile/gaps` bearer tokens using configurable HMAC-SHA256 JWT (or static/env-injected scoped token verifier for internal cluster ops) enforcing `audit:reconcile` scope and `tenant_id` claim isolation, and support an optional dedicated `METRICS_ADDR` listener per `.ai/security.md`.

---

## 4. Verification Steps (Post-Implementation)

1. `gofmt -s -w .`
2. `go vet ./...` and `golangci-lint run`
3. `govulncheck ./...` and `gosec ./...`
4. `go test -v ./pkg/... ./internal/... ./cmd/...` (plus `-race` in CI / Linux)
5. `go test -run=^$ -bench=. -benchmem ./pkg/redaction/...` (assert $\le 128\text{ B/op}$)
6. Sync `.ai/doc-api.md`, `api/openapi/openapi.yaml` (if any contract changes), and `.ai/progress-tracker.md`.
7. Commit in $\ge 2$ conventional git commits (`feat:`, `tests:`, `docs:`).
