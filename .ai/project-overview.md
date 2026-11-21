# RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`) - Project Overview

## Overview

The **`rag-telemetry-audit-daemon`** is the horizontally scalable, asynchronous audit, query logging, and compliance collection engine of the disaggregated RAG architecture. It captures retrieval provenance triplets, access/mutation compliance logs, and token/actor accounting out-of-band via bounded buffers and Apache Kafka, ensuring immutable HIPAA/SOC 2 durability without degrading sub-50ms retrieval paths.

```text
REVISED TELEMETRY & AUDIT DUAL-PLANE
rag-search-orchestrator / Edge Gateway / Control Plane Services
│
├── [Hot Path: Access & Mutate Logs] ────► Local In-Process Bounded Ring Buffer
│                                           │ (Fail-closed / Backpressure on Audit)
│                                           ▼
│                                       Kafka Topic: "audit-compliance-records"
│                                       (Partitioned by: tenant_id)
│                                           │
│                                           ▼
│                                       ┌───────────────────────────────┐
│                                       │ rag-telemetry-audit-daemon    │
│                                       │ (Stateless Kafka Consumer)      │
│                                       └───────┬───────────────┬───────┘
│                                               │               │
│                          ClickHouse RowBinary │ (P99 <= 2s)   │ Micro-Batched Parquet (P99 <= 60s)
│                                               ▼               ▼
│                                       ┌────────────┐  ┌───────────────────────────┐
│                                       │ ClickHouse │  │ AWS S3 (WORM Locked)      │
│                                       │ (Forensics)│  │ (SOC 2/HIPAA Cold Vault)  │
│                                       └────────────┘  └───────────────────────────┘
│
└── [Diagnostic Path: Traces/Waterfalls] ─► OpenTelemetry Collector (Tail-Sampled)
                                            │ (Droppable under duress)
                                            ▼
                                        rag-trace-collector-worker (Python / Ragas)
```

---

## Strategic Objectives

1. **Dual-Track Ingestion Isolation**: Structurally separate compliance access events (SOC 2, HIPAA, write-plane mutations) from high-cardinality diagnostic spans (OTel waterfalls, score decompositions, token counts). Access logs require strict durability; performance traces are prioritized for low latency and may be sampled or dropped under load.
2. **Cloud-Native Immutability**: Replace internal Merkle chaining with AWS S3 Object Lock in **Compliance Mode (WORM)** paired with KMS customer-managed keys (CMK) and AWS CloudTrail data event auditing.
3. **Partitioned Batch Sequencing**: Replace global serialization with strict per-stream monotonically increasing sequence IDs (`[min_seq, max_seq]`) scoped by `tenant_id` and Kafka partition.
4. **Amortized Memory Management**: Use reusable byte slice pools (`sync.Pool`) on all data manipulation and token masking paths to limit allocations during high-volume ingestion ($\le 128\text{ B/op}$), rather than aiming for zero allocations across dynamic text payloads.
5. **Fast Forensic Search**: Stream raw redacted events directly into ClickHouse via RowBinary for real-time investigation and compliance reporting without relying on expensive S3 reads.
6. **Continuous Documentation & Contract Synchronization**: Ensure OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`) and [`.ai/doc-api.md`](doc-api.md) are continuously synchronized with every code change or addition, no matter how small.

---

## Calibrated Quantitative SLIs & SLOs

- **ClickHouse Hot Ingestion Lag**: **P99 $\le 2\text{ seconds}$** from Kafka emission to indexed state in ClickHouse (flushed every `1,000 ms` or `2,500` records).
- **S3 WORM Cold Durability Lag**: **P99 $\le 60\text{ seconds}$** (micro-batches flush every `60 seconds` or when reaching `32 MiB`, preventing S3 API rate limiting).
- **Compliance Ingress Throughput**: Sustained cluster ingestion of **$\ge 35,000\text{ compliance events/sec}$** aggregated across all Kafka partitions without dropped access frames.
- **Allocations Target**: Amortized memory budget **$\le 128\text{ bytes/op}$** during parsing and token redaction across unstructured natural language retrieval chunks.
- **Pod Sizing & Memory Ceiling**:
  - CPU Request / Limit: `1000m` / `2000m`
  - Memory Request / Limit: `1 GiB` / `2 GiB` (`GOMEMLIMIT=1800MiB`, `GOGC=80`, RSS $\le 1.5\text{ GiB}$ under load/spillover)
  - Ephemeral NVMe Storage (`emptyDir`): `10 GiB` ceiling (`5 GiB` upper bound for `mmap_wal` spillover)
  - Topology: Single stateless Kubernetes Deployment (`3–8` replicas via HPA targeting Kafka consumer lag and `70%` CPU).

---

## Scope & Exclusions

### In-Scope
- **Kafka-Only Compliance Ingestion**: Horizontally scalable Kafka consumer group subscribed to `audit-compliance-records` (Apache Kafka 3.7+, `min.insync.replicas=2`, `acks=all`, 7-day retention).
- **Caller-Side Graceful Degradation Library (`pkg/auditclient`)**: Shared Go SDK imported by upstream services (`rag-search-orchestrator`, Edge Gateway, `rag-kb-catalog-service`):
  - *Compliance Channel*: Bounded thread-local memory buffer that enforces backpressure and fails closed (`HTTP 503`) on regulated write-plane mutation events if Kafka/storage stalls.
  - *Diagnostic Channel*: Bounded ring buffer that sheds load (drops spans) during system bottlenecks to preserve search latency SLOs.
- **Tenant-Partitioned In-Flight Sanitization (`pkg/redaction`)**: Masking PII/PHI using compiled Aho-Corasick patterns and byte-pooled scanners (`pkg/pool/byte_pool.go`) prior to committing records to ClickHouse and S3.
- **Partition-Scoped Monotonic Manifests**: Writing sequence intervals (`[min_seq, max_seq]`) directly into S3 Parquet object metadata to support gap detection (`cmd/ledger-verify` & `reconcile_partition_gaps`).
- **Downstream Persistence & NVMe WAL Spillover**: Micro-batching RowBinary streams into ClickHouse (`ReplicatedMergeTree`), flushing columnar Parquet structures to S3 WORM buckets, and spilling to `mmap_wal` (`5 GiB` cap) during downstream outages.

### Exclusions (Explicitly Out-of-Scope)
- **Diagnostic Span Ingestion**: High-volume tracing spans and score distributions route directly to an OpenTelemetry Collector and `rag-trace-collector-worker` (Python / Ragas) for evaluation, bypassing the compliance daemon.
- **Custom Merkle Block Calculations**: Application-level Merkle tree consensus and hash chaining are removed, relying on AWS CloudTrail and S3 Object Lock for non-repudiation.
- **Unix Domain Socket (UDS) Multiplexing**: UDS listeners are removed from the daemon. Local services route records asynchronously through Kafka, removing the need for hybrid DaemonSet configurations.
