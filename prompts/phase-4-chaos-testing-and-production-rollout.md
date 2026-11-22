# Phase 4 Implementation Plan: Chaos Testing & Production Rollout

**Status**: Proposed (Awaiting Human-in-the-Loop Confirmation)  
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

Implement **Phase 4: Chaos Testing & Production Rollout (Weeks 7–8)** from [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), including pressure-tested hardening fixes:

1. **Observability & Port-Isolation Hardening (Pre-Rollout Fixes)**:
   - Add `audit_sequence_gaps_total` counter to `internal/infrastructure/observability/metrics.go` (incremented on partition sequence gaps in `ProcessKafkaBatch` / `ReconcilePartitionGaps`) so the `MissingSequenceGap` `PrometheusRule` alert has a real PromQL backing metric.
   - Update `internal/infrastructure/transport/http/server.go` so that when `METRICS_ADDR` is non-empty (e.g., `:9090`), `GET /metrics` is served **only** on `metricsServer` (`:9090`) and not exposed on the admin/probe port (`:8080`), enforcing L3/L4 `NetworkPolicy` isolation per `.ai/security.md`.
   - Synchronize `.ai/doc-api.md` and `api/openapi/openapi.yaml` with `audit_sequence_gaps_total` and the dedicated `:9090` metrics listener.

2. **Integration Testing (`test/integration/`)**:
   - Build-tagged (`//go:build integration`) Testcontainers suite spinning up ephemeral Apache Kafka, ClickHouse, and LocalStack (`SERVICES=s3,kms` with Object Lock enabled and a provisioned KMS CMK).
   - Provisions ClickHouse using the `.ai/doc-api.md` schema (single-node `ReplacingMergeTree(ingested_at)` in ephemeral container or Keeper-backed `ReplicatedReplacingMergeTree`) and asserts `FINAL` deduplication on reads.
   - Emits `50,000` `AuditRecord` events to `audit-compliance-records` and asserts:
     - ClickHouse indexes the batch within $\le 2\text{ s}$.
     - S3 receives Parquet objects within $\le 60\text{ s}$ with `COMPLIANCE` Object Lock retention headers and `[min_seq, max_seq]` metadata.
     - Zero compliance frames dropped across either tier.
   - Partition rebalance durability test: forces a Kafka consumer group rebalance mid-batch and verifies offsets commit only after both ClickHouse and S3 confirm persistence, with zero duplicate rows under `FINAL`.

3. **Chaos & Recovery Testing (`test/chaos/`)**:
   - **Downstream S3 Rate Limiting / Outage**: Simulates S3 `503 SlowDown` and `500 Internal Error`, asserting traffic spills to `mmap_wal` (`5 GiB` cap) without unbounded heap/RSS growth ($\le 1.5\text{ GiB}$), and drains/recovers once S3 stabilizes.
   - **ClickHouse Failover**: Simulates ClickHouse outages during active writes, asserting Kafka offset advancement halts, records spool to `mmap_wal`, and retries with backoff without dropping in-flight data.
   - **Kafka Broker Restarts & Rebalance Drops**: Simulates broker disconnects/restarts and partition revocation mid-batch under active load, asserting consumer group recovery, offset-commit-after-persist safety, and zero dropped compliance frames.
   - **WAL Saturation & Caller Backpressure Under Heavy Traffic**: Verifies `ErrWALFull` handling and `pkg/auditclient` write-plane fail-closed (`ErrAuditUnavailable` / `HTTP 503`) vs. read-plane `90%` metadata shedding and diagnostic `ring_buffer` drops under sustained concurrent load.

4. **Performance & Memory Validation (`test/performance/`)**:
   - Sustained $\ge 35,000\text{ events/sec}$ ingestion + redaction + batching validation (run **without** `-race` so race-detector overhead does not skew throughput/RSS) confirming memory stability well under the `1.5 GiB` RSS / `GOMEMLIMIT=1800MiB` ceiling and `pkg/redaction` $\le 128\text{ B/op}$ allocation budget.

5. **Unit Verification (`test/unit/`)**:
   - Cross-package black-box unit tests covering end-to-end redaction efficiency, partition sequence monotonicity, and caller SDK backpressure invariants.

6. **Distroless Container & Helm Production + Canary Rollout (`deployments/docker/`, `deployments/helm/`)**:
   - `deployments/docker/Dockerfile`: Multi-stage static Go build (`CGO_ENABLED=0`) on `gcr.io/distroless/static:nonroot` (UID `65532`), shipping `/audit-daemon` and `/ledger-verify`.
   - `deployments/helm/rag-telemetry-audit-daemon/`:
     - Standard Kubernetes `Deployment` (ADR-004, `3` replicas default) with `1000m/2000m` CPU, `1Gi/2Gi` memory, `GOMEMLIMIT=1800MiB`, `GOGC=80`, `readOnlyRootFilesystem: true`, `runAsNonRoot: true` (`65532`), `capabilities: drop: ["ALL"]`, and `10Gi` `emptyDir` mounted at `/var/lib/audit-daemon/wal` (matching `WAL_DIR`, `5 GiB` cap via `WAL_MAX_BYTES`).
     - Full 12-factor env & Secret wiring matching `cmd/audit-daemon/main.go`: `HTTP_ADDR=:8080`, `METRICS_ADDR=:9090`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `CLICKHOUSE_ADDR`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_TLS`, `S3_BUCKET`, `S3_REGION`, `S3_KMS_KEY_ID`, `S3_RETENTION_YEARS`, `RECONCILE_RATE_LIMIT_PER_MIN`, plus `existingSecret` / `secretKeyRef` for `CLICKHOUSE_PASSWORD`, `AUTH_JWT_SECRET`, and `STATIC_ADMIN_TOKEN`.
     - **Canary Cutover Support**: `values-canary.yaml` (and `canary.enabled` / `replicaCount: 1` override sharing the `rag-telemetry-audit-daemon` consumer group on a subset of partitions) plus documented cutover/rollback gates (zero `HotPersistSLOBreach`, zero `MissingSequenceGap`, `audit_wal_spillover_active == 0`, lag draining).
     - `HPA` (`3–8` replicas, `70%` CPU target, plus optional/configurable external `audit_consumer_lag_records` metric for clusters with Prometheus Adapter).
     - `Service` + `NetworkPolicy` isolating `:9090` (`/metrics` to Prometheus scraper) and `:8080` (probes/admin), and restricting egress to Kafka, ClickHouse, configurable S3/KMS CIDRs (or `0.0.0.0/0:443`), and DNS (`UDP`/`TCP` `:53`).
     - `PrometheusRule` defining the 4 production alerts (`HighConsumerLag` `> 15000` for `3m`, `HotPersistSLOBreach` P99 `> 2.0s` for `2m`, `ActiveDiskSpillover` `== 1` for `5m`, `MissingSequenceGap` via `audit_sequence_gaps_total`).

---

## 2. Planned Files

| File | Purpose |
| :--- | :--- |
| `internal/infrastructure/observability/metrics.go` | Add `audit_sequence_gaps_total` counter |
| `internal/application/command/process_kafka_batch.go` | Increment `audit_sequence_gaps_total` on detected sequence gaps |
| `internal/application/query/reconcile_partition_gaps.go` | Increment `audit_sequence_gaps_total` when reconciliation finds gaps |
| `internal/infrastructure/transport/http/server.go` | Serve `/metrics` exclusively on `METRICS_ADDR` when configured |
| `.ai/doc-api.md` & `api/openapi/openapi.yaml` | Document `audit_sequence_gaps_total` and `:9090` metrics port |
| `test/unit/pipeline_invariants_test.go` | Black-box unit tests for redaction, sequence monotonicity, and SDK backpressure |
| `test/chaos/resilience_chaos_test.go` | Chaos tests for S3 503/500 spillover, ClickHouse failover, Kafka broker drops, WAL/SDK backpressure |
| `test/performance/throughput_test.go` | 35,000 events/sec throughput & memory ceiling validation |
| `test/integration/pipeline_integration_test.go` | Testcontainers (Kafka + ClickHouse + LocalStack S3 Object Lock/KMS) suite |
| `deployments/docker/Dockerfile` | Multi-stage distroless non-root (`65532`) static image |
| `deployments/helm/rag-telemetry-audit-daemon/Chart.yaml` | Helm chart metadata |
| `deployments/helm/rag-telemetry-audit-daemon/values.yaml` | Production defaults (`GOMEMLIMIT=1800MiB`, `GOGC=80`, `10Gi` emptyDir, `3–8` HPA, Secrets, NetworkPolicy) |
| `deployments/helm/rag-telemetry-audit-daemon/values-canary.yaml` | Canary rollout values (`1` replica, shared consumer group, cutover gates) |
| `deployments/helm/rag-telemetry-audit-daemon/templates/*` | Deployment, Service, HPA, NetworkPolicy, PrometheusRule, ServiceAccount |
| `Makefile` & `.github/workflows/ci.yml` | Add `test-chaos`, `test-perf`, `test-integration`, `docker-build`, `helm-lint` targets and run `test/chaos` in CI |

---

## 3. Items for Human-in-the-Loop Confirmation

1. **Integration Test Build Tag**: Guard Docker/Testcontainers tests in `test/integration/` with `//go:build integration` (since Docker Desktop is not running locally on this host), while keeping `test/unit/`, `test/chaos/`, and `test/performance/` runnable directly via `go test`.
2. **Helm Chart Configuration Defaults**: Confirm placeholder/configurable values in `values.yaml` for `S3_BUCKET`, `S3_KMS_KEY_ID`, `CLICKHOUSE_ADDR`, `KAFKA_BROKERS`, `existingSecret` (`CLICKHOUSE_PASSWORD`, `AUTH_JWT_SECRET`, `STATIC_ADMIN_TOKEN`), `WAL_DIR=/var/lib/audit-daemon/wal`, `METRICS_ADDR=:9090`, and `HTTP_ADDR=:8080`, plus optional external HPA lag metric (`hpa.kafkaLagMetric.enabled: false` by default for portability).

---

## 4. Verification Plan

1. `gofmt -s -w .`
2. `go vet ./...` and `golangci-lint run`
3. `govulncheck ./...` and `gosec ./...`
4. `npx --yes @stoplight/spectral-cli@6.11.1 lint --ruleset .spectral.yaml api/openapi/openapi.yaml`
5. `go test -v -count=1 ./pkg/... ./internal/... ./cmd/... ./test/unit/... ./test/chaos/...` (and `-race` in CI / Linux)
6. `go test -v -count=1 ./test/performance/...` (without `-race`, confirming $\ge 35,000\text{ events/sec}$ and RSS within ceiling)
7. `go test -run=^$ -bench=. -benchmem ./pkg/redaction/...` (confirming $\le 128\text{ B/op}$)
8. Verify Dockerfile non-root/distroless compliance (`gcr.io/distroless/static:nonroot`, UID `65532`), validate Helm templates (`helm lint` / `helm template` if `helm` is installed), update `.ai/progress-tracker.md`, and commit in $\ge 2$ conventional git commits.
