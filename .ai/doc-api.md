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
| `GET` | `/ready` | Kubernetes Readiness Probe (gated on Kafka consumer group health & drain state) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint | Public / Ops |
| `POST` | `/v1/reconcile/gaps` | Trigger partition sequence `[min_seq, max_seq]` gap reconciliation across ClickHouse & S3 WORM | `audit:reconcile` |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification | Public |
| `GET` | `/docs` | Swagger UI / OpenAPI documentation interface | Public |

---

### Detailed Endpoint Contracts

#### 1. `GET /healthz`, `GET /ready`, `GET /metrics`
- `GET /healthz` $\rightarrow$ `200 OK` `{"status": "alive"}`
- `GET /ready` $\rightarrow$ `200 OK` `{"status": "ready", "checks": {"kafka_consumer": "ok", "clickhouse": "ok", "wal_spillover_active": false}}` or `503 Service Unavailable` (`{"status": "draining"}` during `SIGTERM` shutdown, or `{"status": "not_ready", "checks": {...}}` when Kafka consumer health fails).
- `GET /metrics` $\rightarrow$ Prometheus exposition format (`text/plain; version=0.0.4`).

#### 2. `POST /v1/reconcile/gaps`
Executes `reconcile_partition_gaps.go` to compare `[min_seq, max_seq]` manifests in S3 WORM Parquet metadata against ClickHouse `ReplicatedMergeTree` rows for a given tenant and Kafka partition.

- **Request Headers**:
  - `Authorization: Bearer <token>` *(redacted in all logs)*
  - `Content-Type: application/json`
  - `X-Request-ID: <ulid|uuid>` *(optional; generated if omitted)*
- **Request Body**:
```json
{
  "tenant_id": "tenant-corp-01",
  "partition": 3,
  "from_seq": 100000,
  "to_seq": 125000
}
```
- **Response `200 OK`**:
```json
{
  "tenant_id": "tenant-corp-01",
  "partition": 3,
  "checked_range": {
    "min_seq": 100000,
    "max_seq": 125000
  },
  "gaps": [],
  "status": "consistent"
}
```
- **Response `400 Bad Request`**:
```json
{
  "error": "BAD_REQUEST",
  "message": "Invalid tenant_id, negative partition, or from_seq > to_seq"
}
```

---

## 2. Protobuf v3 & Kafka Ingress Contract (`api/proto/v1/audit_record.proto`)

### Kafka Topic Specification
- **Topic**: `audit-compliance-records`
- **Partition Key**: `hash(tenant_id)`
- **Broker Config**: Apache Kafka 3.7+, `min.insync.replicas=2`, `acks=all`, 7-day log retention.

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
}

message AuditRecord {
  string event_id = 1;                   // ULID/UUID
  string trace_id = 2;
  string tenant_id = 3;
  uint64 seq_id = 4;                     // Monotonic per (tenant_id, kafka_partition)
  google.protobuf.Timestamp event_time = 5;
  PlaneType plane = 6;
  string action = 7;                     // e.g., "search.query", "document.upload", "acl.mutate"
  string service_name = 8;               // e.g., "rag-search-orchestrator"
  ActorContext actor = 9;
  AccessTrace access_trace = 10;
  bytes payload_redacted = 11;           // Sanitized/redacted metadata bytes
  uint32 prompt_tokens = 12;
  uint32 completion_tokens = 13;
}
```

---

## 3. Downstream Persistence Contracts

### ClickHouse Hot Tier (`ReplicatedMergeTree`)
- **Ingestion Protocol**: Native TCP RowBinary stream (`1,000 ms` or `2,500` records; P99 $\le 2\text{ s}$).
- **Table Partitioning & Ordering**:
  - `PARTITION BY toYYYYMM(event_time)`
  - `ORDER BY (tenant_id, event_time, seq_id)`

### AWS S3 WORM Cold Vault (Parquet + Object Lock)
- **Flush Boundary**: `60 seconds` or `32 MiB` (P99 $\le 60\text{ s}$).
- **Object Key Format** (randomized hash prefix prevents S3 `503 SlowDown`):
  - `s3://vault/tenant_<tenant_id>/<hash>/year=YYYY/month=MM/day=DD/p<partition>_<min_seq>_<max_seq>.parquet`
- **Headers & Security**:
  - `x-amz-object-lock-mode: COMPLIANCE`
  - `x-amz-object-lock-retain-until-date: <now + 7 years>`
  - `x-amz-server-side-encryption: aws:kms` (Customer-Managed Key + S3 Bucket Keys)
- **Parquet File Key-Value Metadata**:
  - `tenant_id`, `kafka_partition`, `min_seq`, `max_seq`, `record_count`, `schema_version`
