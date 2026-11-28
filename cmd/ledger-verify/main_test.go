package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

type cliMockHot struct {
	ranges []repository.SequenceRange
}

func (m *cliMockHot) WriteBatch(_ context.Context, _ []entity.ComplianceEntry) error { return nil }
func (m *cliMockHot) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return m.ranges, nil
}
func (m *cliMockHot) Ping(_ context.Context) error { return nil }
func (m *cliMockHot) Close() error                 { return nil }

type cliMockCold struct {
	manifests []repository.ColdObjectManifest
}

func (m *cliMockCold) WriteBatch(_ context.Context, _ entity.BatchManifest, _ []entity.ComplianceEntry) error {
	return nil
}
func (m *cliMockCold) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return m.manifests, nil
}
func (m *cliMockCold) Close() error { return nil }

func TestExecuteVerify_ExitCodes(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := observability.NewMetrics()
	opts := cliOptions{tenantID: "tenant-1", partition: 0, fromSeq: 1, toSeq: 10}

	var out bytes.Buffer
	code := executeVerify(
		context.Background(),
		opts,
		&cliMockHot{ranges: []repository.SequenceRange{{ProducerID: "p1", MinSeq: 1, MaxSeq: 10}}},
		&cliMockCold{manifests: []repository.ColdObjectManifest{{Key: "k1", ProducerID: "p1", MinSeq: 1, MaxSeq: 10}}},
		logger,
		metrics,
		&out,
	)
	if code != 0 || !strings.Contains(out.String(), `"status": "consistent"`) {
		t.Fatalf("expected exit 0 consistent, got %d: %s", code, out.String())
	}

	out.Reset()
	code = executeVerify(
		context.Background(),
		opts,
		&cliMockHot{ranges: []repository.SequenceRange{{ProducerID: "p1", MinSeq: 1, MaxSeq: 10}}},
		&cliMockCold{manifests: []repository.ColdObjectManifest{{Key: "k1", ProducerID: "p1", MinSeq: 1, MaxSeq: 8}}},
		logger,
		metrics,
		&out,
	)
	if code != 2 || !strings.Contains(out.String(), `"status": "inconsistent"`) {
		t.Fatalf("expected exit 2 inconsistent, got %d: %s", code, out.String())
	}
}

func TestRunCLI_ToSeqRequiredFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	// Missing --to-seq must be rejected even when --from-seq 0 matches the default toSeq=0.
	code := runCLI([]string{"--tenant-id", "t1", "--s3-bucket", "b1", "--from-seq", "0"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("expected exit 1 when --to-seq is omitted, got %d", code)
	}

	// Explicit --to-seq 0 with --from-seq 0 passes flag validation (fails later on unreachable S3/CH or succeeds).
	stderr.Reset()
	code = runCLI([]string{
		"--tenant-id", "t1",
		"--s3-bucket", "b1",
		"--from-seq", "0",
		"--to-seq", "0",
		"--clickhouse-addr", "127.0.0.1:1",
		"--timeout", "1",
	}, &stdout, &stderr)
	if strings.Contains(stderr.String(), "--to-seq >= --from-seq are required") {
		t.Fatalf("explicit --to-seq 0 should pass flag validation, got stderr: %s", stderr.String())
	}
	_ = code
}
