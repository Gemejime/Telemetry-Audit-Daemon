# `rag-telemetry-audit-daemon`

Horizontally scalable, asynchronous audit, query logging, and compliance collection engine for the disaggregated RAG platform. It consumes compliance events from Apache Kafka (`audit-compliance-records`), redacts PII/PHI in-flight over pooled byte slices (`<= 128 B/op`), tracks per-producer monotonic sequence intervals (`[min_seq, max_seq]`), and persists records to **ClickHouse** (hot forensic tier) and **AWS S3 Object Lock Compliance Mode** (cold WORM tier), with local NVMe `mmap_wal` spillover during downstream outages.

For system design details, see [ARCHITECTURE.md](ARCHITECTURE.md). For secret injection and Kubernetes Secret management, see [deployments/SECRET_MANAGEMENT.md](deployments/SECRET_MANAGEMENT.md).

---

## Key Capabilities

- **Dual-Plane Caller SDK (`pkg/auditclient`)**:
  - **Compliance Channel**: Bounded channel with `acks=all` Kafka writes; write-plane mutations fail closed (`ErrAuditUnavailable` -> `HTTP 503`), while read-plane queries shed non-essential metadata at 90% fill.
  - **Diagnostic Channel**: Droppable ring buffer (`RingBuffer`) isolating non-compliance spans from compliance durability.
- **Amortized PII/PHI Redaction (`pkg/redaction`, `pkg/pool`)**:
  - Scans and masks PII/PHI directly over `sync.Pool` reusable `[]byte` buffers using Aho-Corasick and compiled patterns.
- **Two-Tier Persistence & NVMe Spillover (`internal/infrastructure/persistence`)**:
  - **Hot Tier (ClickHouse)**: Native TCP RowBinary batching (`1,000 ms` or `2,500` records, P99 <= 2 s).
  - **Cold Tier (AWS S3 WORM)**: Micro-batched Parquet (`60 s` or `32 MiB`, P99 <= 60 s) with Object Lock (Compliance Mode, 7-year retention), KMS CMK, and `[min_seq, max_seq]` metadata.
  - **Spillover (`mmap_wal`)**: Local memory-mapped WAL capped at `5 GiB` so container RSS stays <= `1.5 GiB`.
- **Reconciliation & Admin HTTP (`cmd/ledger-verify`, `internal/infrastructure/transport/http`)**:
  - Exposes `/healthz`, `/ready`, `/metrics`, `/v1/reconcile/gaps` (JWT / static admin token + rate limiting), `/openapi.yaml`, and `/docs`.

---

## Repository Layout

```text
├── api/
│   ├── openapi/openapi.yaml          # OpenAPI 3.1 HTTP specification
│   └── proto/v1/audit_record.proto   # Protobuf v3 AuditRecord / AccessTrace contracts
├── cmd/
│   ├── audit-daemon/main.go          # Main Kafka consumer & HTTP admin/metrics daemon
│   └── ledger-verify/main.go         # CLI for ClickHouse <-> S3 sequence gap verification
├── deployments/
│   ├── docker/Dockerfile             # Distroless non-root (UID 65532) container build
│   ├── helm/rag-telemetry-audit-daemon/
│   │   ├── templates/configmap.yaml  # Dynamic ConfigMap from .Values.env
│   │   ├── templates/deployment.yaml # Deployment with checksum/config & secretKeyRef
│   │   └── values.yaml               # Non-sensitive deployment values (synced from .env.prod)
│   ├── kafka/audit-compliance-records.yaml
│   └── SECRET_MANAGEMENT.md          # Kubernetes Secrets & GitHub Actions injection guide
├── internal/
│   ├── application/                  # Flushers, Kafka batch processor, gap reconciliation
│   ├── domain/                       # ComplianceEntry, SequenceTracker, repository ports
│   └── infrastructure/               # Config (.env loader), Kafka, ClickHouse, S3, WAL, HTTP, metrics
├── pkg/
│   ├── auditclient/                  # Caller-side Go SDK
│   ├── pool/                         # sync.Pool byte buffer pool
│   └── redaction/                    # In-flight []byte PII/PHI redaction engine
├── scripts/
│   └── sync_env_to_deployments.py    # Sync & drift check (.env.prod -> Helm values.yaml)
└── test/                             # Unit, integration, performance, and chaos suites
```

---

## Local Development & Configuration

### 1. Environment Variables (`.env`)

Copy `.env.example` to `.env` for local development:

```bash
cp .env.example .env
```

Both `cmd/audit-daemon`, `cmd/ledger-verify`, and `test/integration` automatically load `.env` at startup via `internal/infrastructure/config/dotenv.go` without overriding variables already exported in your shell.

> **Note**: `.env` is for **local development only**. Deployment configuration is driven by `.env.prod` (gitignored) and synced to `deployments/helm/rag-telemetry-audit-daemon/values.yaml`.

### 2. Running Locally

```bash
# Run the audit daemon
go run ./cmd/audit-daemon

# Run sequence gap verification CLI
go run ./cmd/ledger-verify --tenant-id tenant-1 --partition 0 --from-seq 1 --to-seq 100
```

### 3. Testing, Linting & Benchmarks

```bash
make fmt          # gofmt -s -w .
make vet          # go vet ./...
make test         # go test -v -race -count=1 across pkg/, internal/, cmd/, test/unit/
make bench        # -benchmem allocation checks for pkg/auditclient and pkg/redaction
make openapi-lint # Spectral lint on api/openapi/openapi.yaml
```

---

## Production Deployment & Env Sync

1. Edit `.env.prod` with production non-sensitive settings.
2. Sync `.env.prod` to Helm `values.yaml` (sensitive keys containing `PASSWORD`, `SECRET`, `TOKEN`, or `KEY` — including `S3_KMS_KEY_ID` — as well as `KAFKA_SASL_USERNAME` are automatically excluded):
   ```bash
   python scripts/sync_env_to_deployments.py --sync
   python scripts/sync_env_to_deployments.py --check-only
   ```
3. Install the tracked Git `pre-commit` hook from [`scripts/pre-commit`](scripts/pre-commit) to enforce zero drift between `.env.prod` and `deployments/helm/rag-telemetry-audit-daemon/values.yaml`:
   ```bash
   HOOK_PATH="$(git rev-parse --git-path hooks/pre-commit)" && cp scripts/pre-commit "$HOOK_PATH" && chmod +x "$HOOK_PATH"
   ```
4. CI/CD (`.github/workflows/deploy.yml`) injects `CLICKHOUSE_PASSWORD`, `AUTH_JWT_SECRET`, and optional `STATIC_ADMIN_TOKEN` into the `rag-telemetry-audit-daemon-secrets` Kubernetes Secret, passes the non-secret `S3_KMS_KEY_ID` GitHub repository/environment variable (`vars.S3_KMS_KEY_ID`, when set) to Helm as `env.S3_KMS_KEY_ID` (rendered into the ConfigMap/pod environment), and deploys the Helm chart.
