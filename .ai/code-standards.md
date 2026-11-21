# Code Standards - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

## General Principles

- **Dual-Plane Ingestion Boundary**: Strictly isolate compliance ingestion (`audit-compliance-records` Kafka topic $\rightarrow$ ClickHouse + S3 WORM) from high-cardinality diagnostic traces (OpenTelemetry Collector $\rightarrow$ `rag-trace-collector-worker`). Never add OTel waterfall ingestion or UDS listeners to this daemon.
- **Fail-Closed Write-Plane, Graceful Read-Plane & Downstream Resilience**:
  - In `pkg/auditclient`: Write-plane mutations enforce synchronous `acks=all` Kafka queueing with bounded wait limits and return `HTTP 503` on broker unavailability. Read-plane audits queue non-blockingly and shed non-essential metadata at `90%` buffer fill while preserving the principal access event. Diagnostic `ring_buffer.go` safely drops spans under duress.
  - In `rag-telemetry-audit-daemon`: On ClickHouse or S3 outages (`503 SlowDown`, `500`), halt Kafka offset advancement, spill records to local NVMe `mmap_wal` (`5 GiB` cap), and retry with exponential backoff without growing container RSS past `1.5 GiB`.
- **Root-Cause Focus**: Never mask race conditions or downstream stalls with arbitrary `time.Sleep` calls; use bounded `context.WithTimeout` deadlines, `sync.Pool` byte buffers, per-partition sequence tracking, and offset-commit-after-persist guarantees.

---

## Go 1.23+ & Code Quality Standards

- **Runtime Target**: Go 1.23+ (64-bit) with `GOMEMLIMIT=1800MiB` and `GOGC=80` inside `1 GiB` request / `2 GiB` limit pods.
- **Linter & Formatter**:
  - Formatter: `gofmt -s -w .`
  - Vet: `go vet ./...`
  - Linter: `golangci-lint run` (enforcing `errcheck`, `govet`, `staticcheck`, `unused`, `gosec`, `revive`)
- **Standard Library Utilization**:
  - Prefer Go standard library (`net/http`, `context`, `log/slog`, `sync`, `time`, `crypto/sha256`, `syscall`/`mmap`).
  - Context propagation: All functions performing I/O, Kafka, ClickHouse, S3, or WAL operations MUST accept `context.Context` as the first argument and respect `ctx.Done()`.
- **Idiomatic Go Structuring (Hexagonal)**:
  - Keep `internal/domain/` pure Go with **zero network or storage driver dependencies**; communicate with infrastructure via ports in `internal/domain/repository/` (`HotStorePort`, `ColdStorePort`, `WALPort`).
  - Avoid global mutable state; wire dependencies explicitly via constructor functions in `cmd/audit-daemon/main.go`.
  - Package names must be lowercase, short, and singular (e.g., `entity`, `repository`, `command`, `query`, `pool`, `redaction`, `auditclient`, `clickhouse`, `s3`, `disk`).

---

## Amortized Memory & Sequence Tracking Invariants

1. **Byte-Pooled PII/PHI Redaction (`pkg/pool/byte_pool.go`, `pkg/redaction/`)**:
   - Use `sync.Pool` for reusable `[]byte` buffers; always reset slice lengths (`buf = buf[:0]`) before returning to the pool.
   - Operate directly on `[]byte` using compiled Aho-Corasick patterns and pre-compiled regexes **without casting to Go `string` instances** until final persistence.
   - Amortized memory allocation budget MUST remain **$\le 128\text{ B/op}$** across variable-length JSON and natural language document chunks (`go test -bench=. -benchmem ./pkg/redaction/...`).
2. **Partition-Scoped Monotonic Sequence Tracking (`internal/domain/entity/batch_manifest.go`)**:
   - Track continuous sequence intervals (`[min_seq, max_seq]`) per `(tenant_id, kafka_partition)` and flag any sequence gap before committing to storage.
   - Write `[min_seq, max_seq]` directly into S3 Parquet object key-value metadata for `cmd/ledger-verify` and `reconcile_partition_gaps.go`.
3. **Two-Tier Flush & Spillover Bounds**:
   - ClickHouse RowBinary: flush every `1,000 ms` or `2,500` records (P99 $\le 2\text{ s}$).
   - S3 WORM Parquet: flush every `60 s` or `32 MiB` (P99 $\le 60\text{ s}$).
   - Commit Kafka offsets **only after** both ClickHouse row writes and S3 object uploads succeed.
   - NVMe `mmap_wal.go`: hard-capped at `5 GiB` on a `10 GiB` ephemeral `emptyDir` volume.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs must be emitted as machine-parseable JSON lines via `log/slog`. Never use `fmt.Print*` or `log.Print*` in production packages (`cmd/`, `internal/`, `pkg/`).
- **Mandatory Correlation IDs**: Every log message must include `trace_id`, `tenant_id`, and `partition` (plus `min_seq`, `max_seq`, and `duration_ms` where applicable).
- **Strict Redaction**:
  - Never log `Authorization` headers, bearer tokens, Kafka SASL/TLS secrets, ClickHouse passwords, AWS IAM/KMS credentials, or internal DSNs.
  - Never log unredacted compliance payloads or raw tenant PII/PHI.
  - Sanitize error messages returned on HTTP endpoints (`/ready`, `/v1/reconcile/gaps`) — no internal filesystem paths, hostnames, or stack traces.

---

## Doc API & OpenAPI Mandatory Maintenance

- **Continuous Synchronization**: Whenever any endpoint, request/response field, Protobuf message (`api/proto/v1/audit_record.proto`), Kafka payload, ClickHouse/S3 metadata contract, or metric is added or modified — no matter how small — [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated immediately in the same change step.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
