.PHONY: fmt vet lint vuln sec test bench proto openapi-lint all

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

bench:
	go test -run=^$$ -bench=. -benchmem ./pkg/auditclient/... $$( [ -d pkg/redaction ] && echo ./pkg/redaction/... )

proto:
	buf lint
	buf generate

openapi-lint:
	npx --yes @stoplight/spectral-cli@$(SPECTRAL_VERSION) lint --ruleset .spectral.yaml api/openapi/openapi.yaml

all: fmt vet lint vuln sec test bench proto openapi-lint
