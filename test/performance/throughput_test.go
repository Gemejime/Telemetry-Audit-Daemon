package performance_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/application/command"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/pkg/pool"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

type noopHotStore struct{}

func (noopHotStore) WriteBatch(_ context.Context, _ []entity.ComplianceEntry) error { return nil }
func (noopHotStore) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return nil, nil
}
func (noopHotStore) Ping(_ context.Context) error { return nil }
func (noopHotStore) Close() error                 { return nil }

type noopColdStore struct{}

func (noopColdStore) WriteBatch(_ context.Context, _ entity.BatchManifest, _ []entity.ComplianceEntry) error {
	return nil
}
func (noopColdStore) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return nil, nil
}
func (noopColdStore) Close() error { return nil }

func TestPerformance_Sustained35kEventsPerSecAndMemoryCeiling(t *testing.T) {
	const (
		totalEvents = 105_000
		batchSize   = 500
		minEPS      = 35_000.0
		maxRSSBytes = 1536 * 1024 * 1024 // 1.5 GiB ceiling
	)

	ipHash := sha256.Sum256([]byte("192.0.2.10"))
	ipHex := hex.EncodeToString(ipHash[:])
	now := timestamppb.Now()

	// Pre-build reusable protobuf templates to isolate daemon ingestion/redaction/batching throughput.
	batches := make([][]command.RawKafkaRecord, totalEvents/batchSize)
	seq := uint64(1)
	for b := range batches {
		batch := make([]command.RawKafkaRecord, batchSize)
		for i := range batch {
			pb := &auditv1.AuditRecord{
				EventId:     fmt.Sprintf("evt-%d", seq),
				TraceId:     fmt.Sprintf("tr-%d", seq),
				TenantId:    "tenant-perf-01",
				ProducerId:  "perf-prod-1",
				SeqId:       seq,
				EventTime:   now,
				Plane:       auditv1.PlaneType_PLANE_TYPE_READ,
				Action:      "search.query",
				ServiceName: "rag-search-orchestrator",
				Actor: &auditv1.ActorContext{
					ActorId:      "perf-user",
					TenantId:     "tenant-perf-01",
					AuthMethod:   "jwt",
					SourceIpHash: ipHex,
				},
				PayloadRedacted: []byte(`{"query":"hybrid search for user@example.com with token=secret123"}`),
			}
			raw, err := proto.Marshal(pb)
			if err != nil {
				t.Fatalf("proto.Marshal: %v", err)
			}
			batch[i] = command.RawKafkaRecord{
				Partition: uint16(seq % 8), // #nosec G115
				Offset:    seq,
				Value:     raw,
			}
			seq++
		}
		batches[b] = batch
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	redactor := redaction.NewEngine(bp)
	hotFlusher := command.NewHotFlusher(noopHotStore{}, command.DefaultHotFlushRecords, command.DefaultHotFlushInterval)
	coldFlusher := command.NewColdFlusher(noopColdStore{}, command.DefaultColdFlushBytes, command.DefaultColdFlushInterval, 4)
	proc := command.NewProcessKafkaBatch(redactor, nil, hotFlusher, coldFlusher, noopHotStore{}, noopColdStore{}, nil, logger, nil)

	ctx := context.Background()
	start := time.Now()
	for _, batch := range batches {
		if err := proc.Handle(ctx, batch); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if err := proc.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	elapsed := time.Since(start)
	eps := float64(totalEvents) / elapsed.Seconds()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	t.Logf("processed %d events in %s (%.0f events/sec), HeapAlloc=%.2f MiB, Sys=%.2f MiB",
		totalEvents, elapsed.Round(time.Millisecond), eps,
		float64(ms.HeapAlloc)/(1024*1024), float64(ms.Sys)/(1024*1024),
	)

	if eps < minEPS {
		t.Fatalf("throughput %.0f events/sec is below required %.0f events/sec", eps, minEPS)
	}
	if ms.Sys > maxRSSBytes {
		t.Fatalf("runtime Sys memory %d bytes exceeds 1.5 GiB ceiling (%d bytes)", ms.Sys, maxRSSBytes)
	}
}

func TestPerformance_RedactionAllocationBudget(t *testing.T) {
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	engine := redaction.NewEngine(bp)
	src := []byte(`{"query":"retrieve documents for alice@example.com and ssn=123-45-6789"}`)
	dst := make([]byte, 0, 256)

	allocs := testing.AllocsPerRun(200, func() {
		dst = engine.RedactInto(dst, src)
	})
	if allocs > 0 {
		t.Fatalf("expected 0 heap allocations with reused dst buffer, got %.2f allocs/run", allocs)
	}
}
