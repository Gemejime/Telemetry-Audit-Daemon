# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/audit-daemon ./cmd/audit-daemon && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/ledger-verify ./cmd/ledger-verify

FROM gcr.io/distroless/static:nonroot

USER 65532:65532

COPY --from=builder /out/audit-daemon /audit-daemon
COPY --from=builder /out/ledger-verify /ledger-verify

EXPOSE 8080 9090

ENTRYPOINT ["/audit-daemon"]
