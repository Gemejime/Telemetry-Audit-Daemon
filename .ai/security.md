# Security Context - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

## Threat Model & Trust Boundaries

- **Untrusted / Variable Ingress Payloads**: Compliance events arrive via Kafka (`audit-compliance-records`) from upstream services (`rag-search-orchestrator`, Edge Gateway, `rag-kb-catalog-service`). Every `AuditRecord` must be validated (`tenant_id`, `seq_id`, `event_time`, `plane`) and sanitized via `pkg/redaction` before touching ClickHouse or S3.
- **Tenant Isolation**: Kafka topic `audit-compliance-records` is partitioned by `hash(tenant_id)`. ClickHouse orders rows by `(tenant_id, event_time, seq_id)`, and S3 keys isolate tenants via `s3://vault/tenant_<id>/<hash>/year=YYYY/...`, preventing cross-tenant data mixing.
- **Cloud-Native Immutability (SOC 2 Type II & HIPAA)**:
  - AWS S3 bucket configured with **Object Lock in Compliance Mode** (`7-year` default retention), bucket versioning enabled, and S3 Bucket Keys enabled with **AWS KMS Customer-Managed Keys (CMK)**.
  - AWS CloudTrail data event auditing enabled on the S3 vault bucket for non-repudiation (replacing deprecated application-level Merkle trees per ADR-001).
- **Write-Plane Accountability**: Write-plane mutations (document upload, delete, ACL changes) must fail closed (`HTTP 503`) in `pkg/auditclient` if Kafka (`acks=all`, `min.insync.replicas=2`) is unavailable, ensuring no unlogged regulated mutations occur.
- **Daemon Runtime**: Isolated stateless microservice running as `nonroot:nonroot` (UID `65532`) in `gcr.io/distroless/static:nonroot` with zero shell utilities and a read-only root filesystem (except mounted `emptyDir` for `mmap_wal`).

---

## Secured Logging Invariants

1. **Structured JSON Telemetry Only**:
   - All logs emitted via `log/slog` in JSON format containing correlation metadata (`trace_id`, `tenant_id`, `partition`, `min_seq`, `max_seq`, `duration_ms`).
2. **Credentials & Secrets Redaction**:
   - `Authorization` headers, bearer tokens, Kafka SASL/TLS passwords, ClickHouse credentials, AWS IAM/KMS secrets, and internal DSNs must be stripped/masked before logging.
3. **PII/PHI Protection**:
   - Never log raw `AuditRecord` payloads, unredacted natural language chunks, or raw actor IP addresses; log only redacted summaries, sequence intervals (`[min_seq, max_seq]`), and counts (`record_count`).
4. **Error Trace Sanitization**:
   - Client/operator-facing HTTP responses (`/ready`, `/v1/reconcile/gaps`) must not expose internal filesystem paths, stack traces, or internal hostnames.

---

## Network & Container Hardening

1. **Distroless Non-Root Container**:
   - Base image: `gcr.io/distroless/static:nonroot`
   - UID: `65532` (`runAsNonRoot: true`, `runAsUser: 65532`)
   - Read-only root filesystem (`readOnlyRootFilesystem: true`) with ephemeral NVMe `emptyDir` (`10 GiB` ceiling) mounted solely for `mmap_wal` (`5 GiB` cap).
   - Drop all Linux capabilities (`capabilities: drop: ["ALL"]`).
2. **Kubernetes Network Policies**:
   - **Ingress**: Kubernetes `NetworkPolicy` operates at L3/L4 and cannot enforce HTTP path restrictions; separate `/metrics` onto a dedicated metrics port (restricted to the Prometheus scraper) and admin/reconciliation endpoints onto a separate port or behind a path-aware proxy (restricted to authorized internal ops/admin callers), while retaining application-level bearer authentication for `/v1/reconcile/gaps`.
   - **Egress**: Allowed only to Apache Kafka (`audit-compliance-records`), ClickHouse (`ReplicatedMergeTree` native TCP), AWS S3 / KMS endpoints, and cluster DNS (`kube-dns`/`CoreDNS` `:53`).
3. **Transport Security**:
   - TLS/mTLS for Kafka consumer connections and ClickHouse native TCP; HTTPS/TLS 1.3 for AWS S3 + KMS API calls.

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
