# AI Workflow Rules - RAG Telemetry & Audit Daemon (`rag-telemetry-audit-daemon`)

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in `tasks/` and context files in [`.ai/`](.) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the 5-layer backend architecture documented in [`.ai/architecture.md`](architecture.md) aligned with `tasks/Production Grade Backend Concepts.pdf`.
- Implement against these specs without inferring undocumented dependencies or deviating from architectural decision records (ADR-001 through ADR-004).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan in `prompts/<task-name>.md` (or concisely in response) before making substantial code changes, and await confirmation when requirements are ambiguous.
- **Ambiguity Escalation**: If Kafka topic ACLs, ClickHouse cluster DSNs/schemas, S3 KMS key ARNs, or Protobuf field numbers are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, breaking published Protobuf v3 contracts, or modifying S3 Object Lock retention policies.
- **Demonstrate & Verify**: Provide clear verification steps, unit/benchmark/race test output, and linting checks after completing any unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` MUST be updated on **every code change or addition**, regardless of how small the modification is, whenever any endpoint, schema, Protobuf contract, Kafka payload, ClickHouse table, or S3 Parquet metadata contract changes.
2. **Secured Logging**: All logs emitted by `rag-telemetry-audit-daemon` must be structured JSON (`log/slog`). Strictly redact `Authorization` headers, bearer tokens, Kafka/ClickHouse/AWS credentials, DSNs, and unredacted PII/PHI payloads before emission.
3. **Dual-Track Ingestion Isolation**: Route compliance logs exclusively through Kafka (`audit-compliance-records`); never ingest diagnostic OTel waterfalls or add UDS listeners.
4. **Cloud-Native WORM & Monotonic Sequencing**: Use AWS S3 Object Lock (Compliance Mode, 7-year retention, KMS CMK) instead of application Merkle trees. Commit Kafka offsets only after ClickHouse and S3 persistence succeed.
5. **Memory & Spillover Discipline**: Keep `pkg/redaction` allocations $\le 128\text{ B/op}$ via `sync.Pool` byte slices (`[]byte`), and bound `mmap_wal` spillover at `5 GiB` so container RSS stays $\le 1.5\text{ GiB}$.
6. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
7. **Quality Gates**: `gofmt -s`, `go vet`, `golangci-lint`, `govulncheck`, `gosec`, `-race` tests, and `-benchmem` redaction benchmarks must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting**: Run `gofmt -s -w .` (zero unformatted files).
2. **Static Analysis & Linting**: Run `go vet ./...` and `golangci-lint run`.
3. **Security & Secured Logging Audit**: Run `govulncheck ./...`, `gosec ./...`, and verify no `fmt.Print*`, bearer tokens, credentials, DSNs, or unredacted PII/PHI are logged.
4. **Automated Tests & Benchmarks**: Ensure unit/race tests pass (`go test -v -race ./...`) and `pkg/redaction` benchmarks stay $\le 128\text{ B/op}$.
5. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any new or modified endpoints, schemas, protos, or storage contracts.
6. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.
