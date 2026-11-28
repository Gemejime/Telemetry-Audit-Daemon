# Documented APIs & Contracts - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

This document is the authoritative specification for all public and internal interfaces of the `rag-telemetry-audit-daemon` microservice.

> [!IMPORTANT]
> **Continuous Doc API & OpenAPI 3.1 Updates**: This file and `api/openapi/openapi.yaml` MUST be kept synchronized on **every code change or addition**, regardless of how small the modification is.

---

## 1. OpenAPI 3.1 HTTP Admin, Probe & Reconciliation Specification

### Base URL
`http://localhost:8080` (Internal Service / Cluster Ops)

### Endpoints Overview

| Method | Endpoint | Description | Auth Scope |
| :--- | :--- | :--- | :--- |
| `GET` | `/healthz` | Kubernetes Liveness Probe | Public |
| `GET` | `/ready` | Kubernetes Readiness Probe (gated on drain state, Kafka consumer group health & ClickHouse `PingClickHouse` health) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint (served on dedicated `METRICS_ADDR`, e.g. `:9090`, when configured) | Public / Ops |
| `POST` | `/v1/reconcile/gaps` | Trigger partition sequence `[min_seq, max_seq]` gap reconciliation across ClickHouse & S3 WORM | `audit:reconcile` |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification | Public |
| `GET` | `/docs` | Swagger UI / OpenAPI documentation interface | Public |

---

### Detailed Endpoint Contracts

#### 1. `GET /healthz`, `GET /ready`, `GET /metrics`
- `GET /healthz` $\rightarrow$ `200 OK` `{"status": "alive"}`
- `GET /ready` $\rightarrow$ `200 OK` `{"status": "ready", "checks": {"kafka_consumer": "ok", "clickhouse": "ok", "wal_spillover_active": false}}` or `503 Service Unavailable` (`{"status": "draining"}` during `SIGTERM` shutdown, or `{"status": "not_ready", "checks": {...}}` when Kafka consumer health or ClickHouse `PingClickHouse` health fails).
- `GET /metrics` $\rightarrow$ Prometheus exposition format (`text/plain; version=0.0.4`). Exposes `audit_records_consumed_total`, `audit_consumer_lag_records`, `audit_hot_persist_duration_seconds`, `audit_cold_flush_duration_seconds`, `audit_caller_dropped_records_total`, `audit_wal_spillover_active`, `audit_s3_upload_errors_total`, `audit_s3_interval_overlaps_total`, `audit_malformed_records_total`, and `audit_sequence_gaps_total`. When `METRICS_ADDR` (e.g., `:9090`) is set, `/metrics` is served exclusively on that dedicated port for Kubernetes `NetworkPolicy` isolation.

#### 2. `POST /v1/reconcile/gaps`
Executes `reconcile_partition_gaps.go` to compare `[min_seq, max_seq]` manifests in S3 WORM Parquet metadata against ClickHouse `ReplicatedMergeTree` rows for a given tenant and Kafka partition.

- **Request Headers**:
  - `Authorization: Bearer <token>` *(redacted in all logs)*
  - `Content-Type: application/json`
  - `X-Request-ID: <ulid|uuid>` *(optional; generated if omitted)*
- **Request Body** (sequence fields are base-10 decimal strings representing `uint64` values in `["0", "18446744073709551615"]` to preserve full 64-bit precision in JSON):
```json
{
  "tenant_id": "tenant-corp-01",
  "partition": 3,
  "from_seq": "100000",
  "to_seq": "125000"
}
```
- **Response `200 OK`**:
```json
{
  "tenant_id": "tenant-corp-01",
  "partition": 3,
  "checked_range": {
    "min_seq": "100000",
    "max_seq": "125000"
  },
  "gaps": [
    {
      "producer_id": "search-orch-7f9b",
      "min_seq": "100420",
      "max_seq": "100425",
      "missing_in": "s3"
    }
  ],
  "status": "inconsistent"
}
```
  - `status` is `"inconsistent"` when `gaps` is non-empty (including records missing from `"s3"` or `"clickhouse"`), and `"consistent"` only when `gaps` is empty (`[]`).
- **Error Responses**:
  - `400 Bad Request`: `{"error": "BAD_REQUEST", "message": "Invalid tenant_id, negative partition, or from_seq > to_seq"}`
  - `401 Unauthorized` / `403 Forbidden`: `{"error": "UNAUTHORIZED", "message": "Missing or insufficient audit:reconcile scope"}`
  - `429 Too Many Requests`: `{"error": "RATE_LIMITED", "message": "Reconciliation rate limit exceeded"}`
  - `503 Service Unavailable`: `{"error": "STORAGE_UNAVAILABLE", "message": "ClickHouse or S3 metadata store temporarily unavailable"}`

---

## 2. Protobuf v3 & Kafka Ingress Contract (`api/proto/v1/audit_record.proto`)

### Kafka Topic Specification
- **Topic**: `audit-compliance-records` (`deployments/kafka/audit-compliance-records.yaml`)
- **Partition Key**: `hash(tenant_id)` (deterministic FNV-1a 32-bit modulo `8` default partitions via `auditclient.PartitionForTenant`)
- **Broker Config**: Apache Kafka 3.7+, `partitions=8`, `replicas=3`, `min.insync.replicas=2`, `acks=all` (`kgo.AllISRAcks()`), `retention.ms=604800000` (7-day log retention).

### `api/proto/v1/audit_record.proto`
Canonical compliance payload emitted by `pkg/auditclient` and consumed by `rag-telemetry-audit-daemon`:
```protobuf
syntax = "proto3";

package rag.audit.v1;

option go_package = "rag-telemetry-audit-daemon/api/proto/v1;auditv1";

import "google/protobuf/timestamp.proto";

enum PlaneType {
  PLANE_TYPE_UNSPECIFIED = 0;
  PLANE_TYPE_READ = 1;   // Search queries (non-blocking, sheds metadata at 90% fill)
  PLANE_TYPE_WRITE = 2;  // Document upload, delete, ACL mutations (fail-closed HTTP 503)
}

message ActorContext {
  string actor_id = 1;
  string tenant_id = 2;
  string auth_method = 3;
  string source_ip_hash = 4; // Salted hash; never raw PII IP unless compliance-mandated
}

message AccessTrace {
  repeated uint32 doc_ids = 1;
  repeated string chunk_ids = 2;
  bool cache_hit = 3;
  string embedding_model_version = 4;
  repeated string document_versions = 5; // Immutable doc version lineage (v1..vn)
}

message AuditRecord {
  string event_id = 1;                   // ULID/UUID
  string trace_id = 2;
  string tenant_id = 3;
  uint64 seq_id = 4;                     // Monotonic per (producer_id, tenant_id, kafka_partition)
  google.protobuf.Timestamp event_time = 5;
  PlaneType plane = 6;
  string action = 7;                     // e.g., "search.query", "document.upload", "acl.mutate"
  string service_name = 8;               // e.g., "rag-search-orchestrator"
  ActorContext actor = 9;
  AccessTrace access_trace = 10;
  bytes payload_redacted = 11;           // Sanitized/redacted metadata bytes
  uint32 prompt_tokens = 12;
  uint32 completion_tokens = 13;
  string producer_id = 14;               // Unique caller pod/instance ID for multi-producer seq monotonicity
}
```

---

## 3. Downstream Persistence Contracts

### ClickHouse Hot Tier (`ReplicatedReplacingMergeTree`)
- **Ingestion Protocol**: Native TCP RowBinary stream (`1,000 ms` or `2,500` records; P99 $\le 2\text{ s}$).
- **Deduplication**: `ReplicatedReplacingMergeTree(ingested_at)` makes consumer-rebalance and 60s S3-hold replays idempotent across `(tenant_id, event_time, producer_id, kafka_partition, seq_id)`. Hot-tier readers must query with `FINAL` (or an equivalent deduplication rule) so replay copies are not returned before background part merges complete.
- **DDL**:
```sql
CREATE TABLE IF NOT EXISTS audit_compliance_records ON CLUSTER '{cluster}'
(
    event_id                 String,
    trace_id                 String,
    tenant_id                LowCardinality(String),
    producer_id              LowCardinality(String),
    kafka_partition          UInt16,
    kafka_offset             UInt64,
    seq_id                   UInt64,
    event_time               DateTime64(3, 'UTC'),
    ingested_at              DateTime64(3, 'UTC') DEFAULT now64(3),
    plane                    Enum8('UNSPECIFIED' = 0, 'READ' = 1, 'WRITE' = 2),
    action                   LowCardinality(String),
    service_name             LowCardinality(String),
    actor_id                 String,
    auth_method              LowCardinality(String),
    source_ip_hash           FixedString(64),
    doc_ids                  Array(UInt32),
    chunk_ids                Array(String),
    document_versions        Array(String),
    cache_hit                Bool,
    embedding_model_version  LowCardinality(String),
    payload_redacted         String,
    prompt_tokens            UInt32,
    completion_tokens        UInt32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/audit_compliance_records', '{replica}', ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, event_time, producer_id, kafka_partition, seq_id);
```

### AWS S3 WORM Cold Vault (Parquet + Object Lock)
- **Flush Boundary**: `60 seconds` or `32 MiB` (P99 $\le 60\text{ s}$).
- **Object Key Format** (randomized hash prefix prevents S3 `503 SlowDown`):
  - `s3://vault/tenant_<tenant_id>/<hash>/year=YYYY/month=MM/day=DD/p<partition>_<min_seq>_<max_seq>.parquet`
- **Headers & Security**:
  - `x-amz-object-lock-mode: COMPLIANCE`
  - `x-amz-object-lock-retain-until-date: <now + 7 years>`
  - `x-amz-server-side-encryption: aws:kms` (Customer-Managed Key + S3 Bucket Keys)
- **Parquet Column Schema**:
  - `event_id` (`BYTE_ARRAY/UTF8`), `trace_id` (`BYTE_ARRAY/UTF8`), `tenant_id` (`BYTE_ARRAY/UTF8`), `producer_id` (`BYTE_ARRAY/UTF8`), `kafka_partition` (`INT32`), `kafka_offset` (`INT64`), `seq_id` (`INT64` / `INT(64, false)`), `event_time_ms` (`INT64/TIMESTAMP_MILLIS`), `plane` (`INT32`), `action` (`BYTE_ARRAY/UTF8`), `service_name` (`BYTE_ARRAY/UTF8`), `actor_id` (`BYTE_ARRAY/UTF8`), `auth_method` (`BYTE_ARRAY/UTF8`), `source_ip_hash` (`BYTE_ARRAY/UTF8`), `doc_ids` (`LIST<INT32>` with `INT(32, false)` elements), `chunk_ids` (`LIST<BYTE_ARRAY>`), `document_versions` (`LIST<BYTE_ARRAY>`), `cache_hit` (`BOOLEAN`), `embedding_model_version` (`BYTE_ARRAY/UTF8`), `payload_redacted` (`BYTE_ARRAY`), `prompt_tokens` (`INT32` / `INT(32, false)`), `completion_tokens` (`INT32` / `INT(32, false)`).
- **Parquet File Key-Value Metadata**:
  - `tenant_id`, `producer_id`, `kafka_partition`, `min_seq`, `max_seq`, `record_count`, `schema_version`

---

## Ingestion Configuration & Validation Notes

- `PRODUCER_TENANT_BINDINGS` (optional CSV of `producer_id=tenant_id` pairs): when configured, `ProcessKafkaBatch` checks `RawKafkaRecord.AuthenticatedPrincipal` (when populated by a trusted upstream/consumer adapter) or falls back to the payload `producer_id`, rejects records whose identity is unbound or mismatched against the payload `tenant_id`, increments `audit_malformed_records_total`, and logs only redacted metadata. Note that Kafka topic `WRITE` ACLs only authorize topic access and do not authenticate payload `producer_id` or `tenant_id`, and the current `ConsumerGroup` fetch path does not populate `AuthenticatedPrincipal`.
