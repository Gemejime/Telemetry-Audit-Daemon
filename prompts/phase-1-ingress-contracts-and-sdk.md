# Phase 1 Implementation Plan: Ingress Contracts & Client SDK

**Status**: Approved Decisions (Ready for Execution)  
**Spec References**:
- [`GEMINI.md`](../GEMINI.md)
- [`.ai/progress-tracker.md`](../.ai/progress-tracker.md)
- [`.ai/doc-api.md`](../.ai/doc-api.md)
- [`.ai/architecture.md`](../.ai/architecture.md)
- [`.ai/code-standards.md`](../.ai/code-standards.md)
- [`.ai/security.md`](../.ai/security.md)
- [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md)
- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)

---

## 1. Goals & Exit Criteria

Implement **Phase 1: Ingress Contracts & Client SDK (Weeks 1–2)** from [`.ai/progress-tracker.md`](../.ai/progress-tracker.md):
1. Initialize the Go 1.23+ module (`go.mod`) and developer `Makefile`.
2. Define canonical Protobuf v3 ingress contracts (`api/proto/v1/audit_record.proto`, `buf.yaml`, `buf.gen.yaml`) and generate `api/proto/v1/audit_record.pb.go` via `buf generate` (`buf` v1.47.2), passing `buf lint`.
3. Define the OpenAPI 3.1.0 HTTP specification (`api/openapi/openapi.yaml`) for `/healthz`, `/ready`, `/metrics`, `/v1/reconcile/gaps`, `/openapi.yaml`, and `/docs`, passing `spectral lint` against [`.spectral.yaml`](../.spectral.yaml) with complete `operationId`, descriptions, tags, and schemas.
4. Implement `pkg/auditclient/` backed by `github.com/twmb/franz-go` behind a narrow `Producer` port interface:
   - **Input Validation & Enrichment** (`client.go`): Validate required fields (`tenant_id`, `plane != PLANE_TYPE_UNSPECIFIED`, `action`, `actor.actor_id`), auto-populate `event_id` (UUID/ULID) and `event_time` when unset, and stamp `producer_id` (unique pod/instance ID).
   - **Deterministic Partitioning & Monotonic `seq_id`** (`client.go`): Compute partition deterministically via `hash(tenant_id)` (matching the `franz-go` custom partitioner) and atomically assign monotonic `uint64 seq_id` per `(producer_id, tenant_id, partition)` **only upon acceptance** into the compliance pipeline (preventing false sequence gaps on failed or dropped calls).
   - **Compliance Channel** (`compliance_channel.go`): Isolate `PLANE_TYPE_WRITE` and `PLANE_TYPE_READ` queues so read-plane spikes cannot starve write-plane mutations:
     - `PLANE_TYPE_WRITE`: Synchronous `acks=all` queueing with bounded wait timeout returning typed sentinel `ErrAuditUnavailable` (mapped to `HTTP 503` by callers).
     - `PLANE_TYPE_READ`: Non-blocking queueing; at `>= 90%` fill, shed non-essential metadata (`access_trace.chunk_ids`, `access_trace.document_versions`, `payload_redacted`, `prompt_tokens`, `completion_tokens`) while preserving the principal access event (`event_id`, `trace_id`, `tenant_id`, `seq_id`, `event_time`, `plane`, `action`, `service_name`, `actor`, `producer_id`); at `100%` fill, drop non-blockingly and increment the drop counter.
   - **Diagnostic Channel** (`ring_buffer.go`): Bounded droppable ring buffer for non-compliance diagnostic spans, decoupled via a `DiagnosticSink` interface (for OTel Collector gRPC export per ADR-002), exposing `audit_caller_dropped_records_total`.
5. Provide Kafka topic configuration (`deployments/kafka/audit-compliance-records.yaml`) for `audit-compliance-records` (`partitions: 8`, `replicas: 3`, `min.insync.replicas: "2"`, `retention.ms: "604800000"` / 7 days; with `acks=all` enforced on the `franz-go` producer).
6. Verify all CI gates (`gofmt -s`, `go vet`, `golangci-lint`, `govulncheck`, `gosec`, `buf lint`, `spectral lint`, `-race` tests, `-benchmem` hot-path benchmarks), update `.ai/doc-api.md` + `.ai/progress-tracker.md`, and commit in at least two conventional git commits.

---

## 2. Planned Files

| File | Purpose |
| :--- | :--- |
| `go.mod` / `go.sum` | Go module `rag-telemetry-audit-daemon` (`go 1.23`) with `franz-go` and `protobuf` |
| `Makefile` | Targets for `fmt`, `vet`, `lint`, `vuln`, `sec`, `test`, `bench`, `proto`, `openapi-lint` |
| `buf.yaml` / `buf.gen.yaml` | Protobuf lint & Go codegen config (`buf` v1.47.2) |
| `api/proto/v1/audit_record.proto` | `PlaneType`, `ActorContext`, `AccessTrace`, `AuditRecord` |
| `api/proto/v1/audit_record.pb.go` | Generated Go Protobuf types & wire serialization |
| `api/openapi/openapi.yaml` | OpenAPI 3.1.0 spec for `/healthz`, `/ready`, `/metrics`, `/v1/reconcile/gaps`, `/openapi.yaml`, `/docs` |
| `pkg/auditclient/client.go` | `Client`, `Config`, `Producer` port, `franz-go` adapter, `hash(tenant_id)` partitioner, `seq_id` tracker, `log/slog` redaction |
| `pkg/auditclient/compliance_channel.go` | Isolated write-plane fail-closed backpressure (`ErrAuditUnavailable`) & read-plane 90% metadata shedding |
| `pkg/auditclient/ring_buffer.go` | Bounded droppable diagnostic ring buffer, `DiagnosticSink` port, and `audit_caller_dropped_records_total` |
| `pkg/auditclient/client_test.go` | Unit & `-benchmem` tests for validation, backpressure, 90%/100% read-plane behavior, ring buffer drops, and `seq_id`/partition determinism |
| `deployments/kafka/audit-compliance-records.yaml` | Kafka topic manifest (`partitions: 8`, `replicas: 3`, `min.insync.replicas: "2"`, 7d retention) |

---

## 3. Security & Secured Logging Compliance

- All logs in `pkg/auditclient/` use `log/slog` JSON output with correlation fields (`trace_id`, `tenant_id`, `partition`).
- No `Authorization` headers, bearer tokens, Kafka SASL/TLS credentials, DSNs, or unredacted `payload_redacted` / PII fields are logged.
- `ErrAuditUnavailable` is a typed sentinel error without internal hostnames, DSNs, or stack traces so upstream HTTP services map it cleanly to `HTTP 503`.

---

## 4. Verification Steps

1. `gofmt -s -w .` (zero unformatted files).
2. `go vet ./...` and `golangci-lint run`.
3. `govulncheck ./...` and `gosec ./...`.
4. `buf lint` and `npx --yes @stoplight/spectral-cli@6.11.1 lint --ruleset .spectral.yaml api/openapi/openapi.yaml`.
5. `go test -v -race -count=1 ./pkg/auditclient/...` and `go test -run=^$ -bench=. -benchmem ./pkg/auditclient/...`.
6. Verify no `fmt.Print*`, `log.Print*`, or sensitive field names appear in `slog` calls.
7. Verify `.ai/doc-api.md` and `api/openapi/openapi.yaml` are synchronized and `.ai/progress-tracker.md` reflects Phase 1 progress.
8. Commit in `>= 2` conventional commits (`feat:`, `docs:`, `tests:`, `chore:`).

---

## 5. Resolved Decisions

1. **Kafka Go Client**: Use **`github.com/twmb/franz-go`** behind a narrow `Producer` port interface in `pkg/auditclient/client.go`, configured with `acks=all` (`AllISRAcks`) and a deterministic `hash(tenant_id)` partitioner.
2. **Protobuf Go Generation**: Use **`buf` (`v1.47.2`) + `buf generate`** (`buf.gen.yaml`) to generate `api/proto/v1/audit_record.pb.go` directly from `api/proto/v1/audit_record.proto` (no hand-written Go struct substitutes).
