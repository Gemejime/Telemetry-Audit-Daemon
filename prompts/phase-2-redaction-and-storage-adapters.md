# Phase 2 Implementation Plan: Redaction Engine & Storage Adapters

**Status**: Completed  
**Spec References**:
- [`GEMINI.md`](../GEMINI.md)
- [`tasks/RAG Telemetry Audit Daemon Plan.pdf`](../tasks/RAG%20Telemetry%20Audit%20Daemon%20Plan.pdf)
- [`.ai/progress-tracker.md`](../.ai/progress-tracker.md)
- [`.ai/architecture.md`](../.ai/architecture.md)
- [`.ai/doc-api.md`](../.ai/doc-api.md)
- [`.ai/code-standards.md`](../.ai/code-standards.md)
- [`.ai/security.md`](../.ai/security.md)
- [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md)

---

## 1. Goals & Exit Criteria

Implement **Phase 2: Redaction Engine & Storage Adapters (Weeks 3–4)** from [`.ai/progress-tracker.md`](../.ai/progress-tracker.md):

1. **Byte-Slice Pool (`pkg/pool/byte_pool.go`)**:
   - `sync.Pool`-backed reusable `[]byte` buffer pool with `Get` / `Put` helpers that reset slice length (`buf = buf[:0]`) and cap maximum retained capacity to prevent heap bloat.
2. **Amortized In-Flight PII/PHI Redaction Engine (`pkg/redaction/engine.go`, `pkg/redaction/patterns.go`)**:
   - Operates directly on `[]byte` without casting to `string`, using pooled `[]byte` scratch buffers from `pkg/pool/byte_pool.go`.
   - Combines a self-contained, zero-allocation pure-Go Aho-Corasick multi-pattern automaton (for sensitive key/prefix triggers such as `Bearer `, `Authorization:`, `api_key`, `password`, `ssn`, `AKIA`) with zero-allocation ASCII byte scanners for structured tokens (emails, SSNs, credit card numbers, phone numbers, JWTs), avoiding `regexp.FindIndex`/`regexp.FindAllIndex` `[]int` heap escapes on the hot path.
   - Enforces **$\le 128\text{ B/op}$** amortized allocations in `go test -run=^$ -bench=. -benchmem ./pkg/redaction/...` across variable-length JSON and natural language chunks, including multi-hit PII/PHI payloads.
3. **Hexagonal Domain Models & Ports (`internal/domain/`)**:
   - `entity/compliance_entry.go`: Pure Go domain entity (`ComplianceEntry`) with zero infrastructure imports, including `kafka_partition` (`uint16`) and `kafka_offset` (`uint64`) from Kafka consumer metadata, and strict validation (`event_id`, `trace_id`, `tenant_id`, `producer_id`, `seq_id`, `event_time`, `plane`, `action`, `actor_id`, and 64-char hex `source_ip_hash` matching ClickHouse `FixedString(64)`).
   - `entity/batch_manifest.go`: Partition-scoped monotonic sequence tracker keyed by `(producer_id, tenant_id, kafka_partition)` that verifies both intra-batch `[min_seq, max_seq]` continuity and cross-batch `expected_next_seq` continuity (with bounded LRU/TTL state eviction to protect the `1.5 GiB` RSS ceiling), detecting and flagging sequence gaps and duplicates before storage commit.
   - `repository/hot_store_port.go` (`HotStorePort` with `WriteBatch`), `repository/cold_store_port.go` (`ColdStorePort`), and `repository/wal_port.go` (`WALPort`).
4. **ClickHouse RowBinary Ingestion Adapter (`internal/infrastructure/persistence/clickhouse/`)**:
   - `client.go`: `github.com/ClickHouse/clickhouse-go/v2` native TCP connection pool, TLS/auth config, and exponential backoff retry wrapper with structured JSON `log/slog` logging (including `trace_id`, `tenant_id`, `partition`, `min_seq`, `max_seq`, `duration_ms`, and strict redaction of DSNs/credentials).
   - `row_binary_writer.go`: Implements `HotStorePort.WriteBatch` writing to `audit_compliance_records` (`ReplicatedReplacingMergeTree`) matching the exact DDL in [`.ai/doc-api.md`](../.ai/doc-api.md), with explicit column-list mapping (consistently handling `ingested_at` `DEFAULT now64(3)`) and latency hooks for `audit_hot_persist_duration_seconds` (P99 $\le 1.5\text{ s}$ / `2\text{ s}` SLO). *(Note: The `1,000 ms` / `2,500`-record flush loop in `internal/application/command/flush_clickhouse.go` is deferred to Phase 3 when Kafka consumer orchestration is wired.)*
5. **Local NVMe `mmap_wal` Spillover (`internal/infrastructure/persistence/disk/`)**:
   - `mmap_wal.go` (shared segmented WAL logic + `mmap_wal_linux.go` with `//go:build linux` `syscall.Mmap` and `mmap_wal_portable.go` with `//go:build !linux` file-backed fallback for Windows/macOS local testing).
   - Uses bounded, rotated WAL segments (e.g., `64 MiB`–`128 MiB` per segment) hard-capped at **`5 GiB`** (`5 * 1024^3` bytes) total, unmapping and deleting drained segments (`munmap` + `os.Remove`) so mapped pages do not grow container RSS past `1.5 GiB`.
   - Returns typed `ErrWALFull` when reaching the `5 GiB` cap (signaling backpressure to halt Kafka offset advancement without dropping compliance frames), frames records with CRC32 (logging only offsets/sequence metadata on corruption, never raw bytes), isolates `gosec`-annotated `unsafe` usage (if any) to Linux `mmap`, and exposes spillover state for `audit_wal_spillover_active`.
6. **Unit Tests & Benchmarks**:
   - Allocation benchmarks (`pkg/redaction`) verifying $\le 128\text{ B/op}$ across single-hit and multi-hit JSON/text payloads.
   - Intra-batch and cross-batch sequence gap, out-of-order, duplicate, and bounded-eviction tests (`internal/domain/entity`).
   - `mmap_wal` `5 GiB` cap/`ErrWALFull` enforcement, segment rotation/drain cleanup, and CRC32 corruption recovery tests.
   - ClickHouse `WriteBatch` column mapping (`FixedString(64)`, `ingested_at`), serialization, and retry/backoff unit tests.

---

## 2. Planned Files

| File | Purpose |
| :--- | :--- |
| `pkg/pool/byte_pool.go` | `sync.Pool` reusable `[]byte` buffers (`Get`, `Put`, max-cap guard) |
| `pkg/redaction/patterns.go` | Zero-alloc Aho-Corasick automaton + ASCII byte scanners for PII/PHI & secrets |
| `pkg/redaction/engine.go` | In-flight `[]byte` redaction engine ($\le 128\text{ B/op}$) |
| `pkg/redaction/engine_test.go` | Unit tests & `-benchmem` benchmarks across single/multi-hit JSON & text chunks |
| `internal/domain/entity/compliance_entry.go` | Pure Go `ComplianceEntry` domain model (`KafkaPartition`, `KafkaOffset`, `FixedString(64)` validation) |
| `internal/domain/entity/batch_manifest.go` | `(producer_id, tenant_id, kafka_partition)` intra/cross-batch gap tracker with bounded eviction |
| `internal/domain/entity/batch_manifest_test.go` | Disordered, duplicate, cross-batch gap, and eviction unit tests |
| `internal/domain/repository/hot_store_port.go` | `HotStorePort` interface (`WriteBatch`) for ClickHouse |
| `internal/domain/repository/cold_store_port.go` | `ColdStorePort` interface for S3 WORM |
| `internal/domain/repository/wal_port.go` | `WALPort` interface (`Append`, `Drain`, `Ack`, `ErrWALFull`) for local spillover |
| `internal/infrastructure/persistence/clickhouse/client.go` | `clickhouse-go/v2` native TCP pool, TLS/auth config, exponential backoff retries |
| `internal/infrastructure/persistence/clickhouse/row_binary_writer.go` | `HotStorePort` RowBinary batch writer for `audit_compliance_records` |
| `internal/infrastructure/persistence/clickhouse/writer_test.go` | RowBinary column mapping, serialization & retry/backoff unit tests |
| `internal/infrastructure/persistence/disk/mmap_wal.go` | Segmented `WALPort` implementation with `5 GiB` cap, `ErrWALFull`, & CRC32 framing |
| `internal/infrastructure/persistence/disk/mmap_wal_linux.go` | Linux `//go:build linux` `syscall.Mmap` / `munmap` segment backend |
| `internal/infrastructure/persistence/disk/mmap_wal_portable.go` | Non-Linux `//go:build !linux` file-backed fallback for Windows/macOS |
| `internal/infrastructure/persistence/disk/mmap_wal_test.go` | `5 GiB` cap/`ErrWALFull`, segment drain/cleanup, and corruption recovery tests |

---

## 3. Architecture Decisions for Human-in-the-Loop Confirmation

1. **Aho-Corasick & Pattern Scanning**: Use a self-contained, zero-allocation pure-Go Aho-Corasick automaton + ASCII byte scanners in `pkg/redaction/patterns.go` (avoiding `regexp.FindIndex` `[]int` heap escapes to guarantee $\le 128\text{ B/op}$).
2. **ClickHouse Driver**: Use `github.com/ClickHouse/clickhouse-go/v2` (official native TCP driver with `PrepareBatch` / RowBinary support) for `internal/infrastructure/persistence/clickhouse/`.
3. **ClickHouse Schema & Phase 3 Boundary**: `row_binary_writer.go` targets `audit_compliance_records` (default database from DSN, matching `.ai/doc-api.md` with explicit column mapping for `ingested_at`), while `internal/application/command/flush_clickhouse.go` (`1,000 ms` / `2,500`-record timer loop) is deferred to Phase 3 when Kafka + S3 WORM are wired together.

---

## 4. Verification Steps (Post-Implementation)

1. `gofmt -s -w .`
2. `go vet ./...` and `golangci-lint run`
3. `govulncheck ./...` and `gosec ./...`
4. `go test -v -race -count=1 ./pkg/... ./internal/...`
5. `go test -run=^$ -bench=. -benchmem ./pkg/redaction/...` (assert $\le 128\text{ B/op}$)
6. Sync `.ai/doc-api.md`, `api/openapi/openapi.yaml` (if any contract changes), and `.ai/progress-tracker.md`.
7. Commit in $\ge 2$ conventional git commits (`feat:`, `tests:`, `docs:`).
