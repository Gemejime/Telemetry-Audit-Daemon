# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/audit-daemon ./cmd/audit-daemon && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/ledger-verify ./cmd/ledger-verify && \
    mkdir -p /out/wal && chown 65532:65532 /out/wal

FROM gcr.io/distroless/static:nonroot

USER 65532:65532

COPY --from=builder /out/audit-daemon /audit-daemon
COPY --from=builder /out/ledger-verify /ledger-verify
COPY --from=builder --chown=65532:65532 /out/wal /var/lib/audit-daemon/wal

EXPOSE 8080 9090

ENTRYPOINT ["/audit-daemon"]
