.PHONY: fmt vet lint vuln sec test test-chaos test-perf test-integration bench proto openapi-lint docker-build all

BUF_VERSION := v1.47.2
SPECTRAL_VERSION := 6.11.1

fmt:
	gofmt -s -w .

vet:
	go vet ./...

lint:
	golangci-lint run

vuln:
	govulncheck ./...

sec:
	gosec ./...

test:
	go test -v -race -count=1 ./pkg/... $$( [ -d internal ] && echo ./internal/... ) $$( [ -d cmd ] && echo ./cmd/... ) $$( [ -d test/unit ] && echo ./test/unit/... )

test-chaos:
	go test -v -race -count=1 ./test/chaos/...

test-perf:
	go test -v -count=1 ./test/performance/...

test-integration:
	go test -v -tags=integration -count=1 ./test/integration/...

docker-build:
	docker build -f deployments/docker/Dockerfile -t rag-telemetry-audit-daemon:latest .

bench:
	go test -run=^$$ -bench=. -benchmem ./pkg/auditclient/... $$( [ -d pkg/redaction ] && echo ./pkg/redaction/... )

proto:
	buf lint
	buf generate

openapi-lint:
	npx --yes @stoplight/spectral-cli@$(SPECTRAL_VERSION) lint --ruleset .spectral.yaml api/openapi/openapi.yaml

all: fmt vet lint vuln sec test bench proto openapi-lint
