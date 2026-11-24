# Secret & Environment Management (`rag-telemetry-audit-daemon`)

This document describes how non-sensitive configuration and sensitive credentials are separated, synchronized, and injected into Kubernetes for `rag-telemetry-audit-daemon`.

---

## 1. Configuration vs. Secrets Separation

| Layer | Source of Truth | Tracked in Git? | Kubernetes Resource | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Local Development** | `.env` (copied from `.env.example`) | No (`.gitignore`) | N/A | Loaded at startup by `config.LoadDotEnv()` in `cmd/audit-daemon`, `cmd/ledger-verify`, and integration tests. |
| **Non-Sensitive Deployment Config** | `.env.prod` $\rightarrow$ `scripts/sync_env_to_deployments.py --sync` | `.env.prod` is **not** tracked; `values.yaml` **is** tracked | `ConfigMap` (`templates/configmap.yaml`) | Automatically excludes keys containing `PASSWORD`, `SECRET`, `TOKEN`, or `KEY`. Pod restarts are triggered automatically via `checksum/config`. |
| **Sensitive Runtime Credentials** | GitHub Actions Environment Secrets | **Never** | `Secret` (`rag-telemetry-audit-daemon-secrets`) | Injected idempotently by `.github/workflows/deploy.yml` and mounted via `secretKeyRef` in `templates/deployment.yaml`. |
| **AWS S3 / KMS Access** | EKS IRSA (IAM Role for Service Account) | Annotation in `values.yaml` | `ServiceAccount` | Preferred over static `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. |

---

## 2. Kubernetes Secret Inventory (`rag-telemetry-audit-daemon-secrets`)

`deployments/helm/rag-telemetry-audit-daemon/templates/deployment.yaml` mounts keys from the Secret named by `.Values.existingSecret` (`rag-telemetry-audit-daemon-secrets`):

| Key | Required | Purpose |
| :--- | :--- | :--- |
| `CLICKHOUSE_PASSWORD` | **Yes** | Native TCP authentication password for the ClickHouse hot tier. |
| `AUTH_JWT_SECRET` | **Yes** | HMAC-SHA256 signing secret used to verify Bearer JWTs on `POST /v1/reconcile/gaps`. |
| `STATIC_ADMIN_TOKEN` | Optional (`optional: true`) | Static admin Bearer token for `POST /v1/reconcile/gaps` when static token auth is enabled. |

> **Note on `S3_KMS_KEY_ID` and AWS Credentials**:
> `scripts/sync_env_to_deployments.py` excludes any key containing `KEY` (`S3_KMS_KEY_ID`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) from `values.yaml`.
> - In production EKS, configure **IRSA** on `serviceAccount.annotations` in `values.yaml` so pods obtain temporary STS credentials without static AWS keys.
> - Pass `S3_KMS_KEY_ID` (non-secret KMS key ARN/alias) via Helm `--set env.S3_KMS_KEY_ID=...` if you override the default KMS key.

---

## 3. CI/CD Secret Injection (GitHub Actions)

The deployment workflow [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) injects Kubernetes Secrets directly from GitHub Actions Secrets before running `helm upgrade --install`:

### Required GitHub Actions Environment Secrets

Configure these under **Settings $\rightarrow$ Environments $\rightarrow$ `production` (or `staging`) $\rightarrow$ Secrets**:

- `KUBE_CONFIG_DATA`: Base64-encoded kubeconfig with permissions to manage Secrets and Helm releases in the target namespace (`base64 -w 0 ~/.kube/config`).
- `CLICKHOUSE_PASSWORD`: ClickHouse writer password.
- `AUTH_JWT_SECRET`: HMAC-SHA256 secret for admin/reconciliation JWTs.
- `STATIC_ADMIN_TOKEN` *(optional)*: Static admin Bearer token if used.

### How Injection Works

1. The workflow validates that `KUBE_CONFIG_DATA`, `CLICKHOUSE_PASSWORD`, and `AUTH_JWT_SECRET` are non-empty without printing any secret values.
2. It runs an idempotent `kubectl create secret generic ... --dry-run=client -o yaml | kubectl apply -f -` to create or update `rag-telemetry-audit-daemon-secrets` in the target namespace.
3. It runs `helm lint` and `helm upgrade --install` with `deployments/helm/rag-telemetry-audit-daemon/values.yaml`.
4. `kubectl rollout status` verifies all pods become ready.

---

## 4. Secret Rotation & Zero-Downtime Rollout

Because Kubernetes does not automatically restart pods when a `Secret` mounted via `env.secretKeyRef` changes:

1. Update the secret value in **GitHub Actions Environment Secrets** (and in ClickHouse / token issuers as needed).
2. Re-run the `CD - Secret Injection & Helm Deploy` workflow (or update the Secret in-cluster) and trigger a rolling restart:
   ```bash
   kubectl rollout restart deployment/rag-telemetry-audit-daemon -n audit-system
   kubectl rollout status deployment/rag-telemetry-audit-daemon -n audit-system
   ```

---

## 5. Production Alternatives (GitOps / KMS)

For clusters using **External Secrets Operator (ESO)** or **Sealed Secrets**:
- Keep `existingSecret: "rag-telemetry-audit-daemon-secrets"` in `values.yaml`.
- Replace the `kubectl create secret` step in `deploy.yml` with an `ExternalSecret` manifest synced from AWS Secrets Manager / Parameter Store, or a `SealedSecret` encrypted with the cluster's public key.

---

## 6. Security & Logging Invariants

- Never commit `.env`, `.env.prod`, `kubeconfig`, or raw credentials to Git.
- Never print environment variables or secret values in GitHub Actions logs (`set -x` must not be enabled on secret-handling steps).
- At runtime, `rag-telemetry-audit-daemon` uses structured JSON logging (`log/slog`) and strictly redacts `Authorization` headers, Bearer tokens, ClickHouse passwords, and AWS credentials.
