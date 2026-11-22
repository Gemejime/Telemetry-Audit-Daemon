# GEMINI.MD — Agent Instructions for `rag-telemetry-audit-daemon`

This repository implements **`rag-telemetry-audit-daemon`**, the horizontally scalable, asynchronous audit, query logging, and compliance collection engine of the disaggregated RAG platform.

Before planning, writing, or modifying any code, configuration, or documentation, you MUST read and follow:

1. **Primary Agent Persona & Execution Protocol**: [`.ai/agents.md`](.ai/agents.md)
2. **Root Context Index**: [`.ai.md`](.ai.md)
3. **Remaining `.ai/` Specification & Governance Files** (read in order):
   - [`.ai/project-overview.md`](.ai/project-overview.md) — Dual-plane telemetry & audit architecture, SLIs/SLOs ($\ge 35,000$ events/sec, ClickHouse P99 $\le 2\text{ s}$, S3 WORM P99 $\le 60\text{ s}$, $\le 128\text{ B/op}$ redaction), and scope boundaries.
   - [`.ai/architecture.md`](.ai/architecture.md) — 5-layer production backend architecture, hexagonal folder structure, Kubernetes pod sizing, and Prometheus alerting matrix.
   - [`.ai/doc-api.md`](.ai/doc-api.md) — OpenAPI 3.1 HTTP endpoints, Protobuf v3 contracts (`api/proto/v1/audit_record.proto`), Kafka `audit-compliance-records` schema, ClickHouse DDL, and S3 WORM Parquet metadata.
   - [`.ai/code-standards.md`](.ai/code-standards.md) — Go 1.23+ idioms, `sync.Pool` byte buffers, Aho-Corasick/regex `[]byte` redaction, `mmap_wal` spillover, and `pkg/auditclient` backpressure rules.
   - [`.ai/security.md`](.ai/security.md) — S3 Object Lock (Compliance Mode), KMS CMK, CloudTrail, tenant isolation, distroless non-root runtime, and Secured Logging invariants.
   - [`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md) — Human-in-the-loop rules, CI/CD quality gates, and pre-commit checklist.
   - [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — Implementation phases (Phases 0–4), ADRs (ADR-001 to ADR-004), and operational runbooks.

---

## Mandatory Cross-Cutting Rules

1. **Continuous Doc API & OpenAPI Sync**: Always update [`.ai/doc-api.md`](.ai/doc-api.md) and `api/openapi/openapi.yaml` whenever any endpoint, schema, Protobuf contract, Kafka topic payload, ClickHouse schema, or S3 metadata contract is created or modified — no matter how small the change is.
2. **Secured Logging**: All runtime logs must use structured JSON (`log/slog`) and strictly redact PII/PHI, `Authorization` headers, bearer tokens, Kafka/ClickHouse/AWS credentials, DSNs, and raw compliance payloads.
3. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
