package unit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/pkg/auditclient"
	"rag-telemetry-audit-daemon/pkg/pool"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

type blockingProducer struct {
	mu      sync.Mutex
	release chan struct{}
	records []*auditv1.AuditRecord
}

func newBlockingProducer() *blockingProducer {
	return &blockingProducer{release: make(chan struct{})}
}

func (p *blockingProducer) ProduceSync(ctx context.Context, _ string, _ int32, _, value []byte) error {
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	var rec auditv1.AuditRecord
	if err := proto.Unmarshal(value, &rec); err == nil {
		p.mu.Lock()
		p.records = append(p.records, &rec)
		p.mu.Unlock()
	}
	return nil
}

func (p *blockingProducer) Close() {}

type countingDiagSink struct {
	exported atomic.Int64
}

func (s *countingDiagSink) Export(_ context.Context, spans []auditclient.DiagnosticSpan) error {
	s.exported.Add(int64(len(spans)))
	return nil
}

func TestEndToEndRedactionInvariants(t *testing.T) {
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	engine := redaction.NewEngine(bp)

	raw := []byte(`{"query":"find docs for alice@example.com ssn=123-45-6789 token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U","password":"super-secret-password"}`)
	out := engine.RedactInPlace(append([]byte(nil), raw...))

	for _, leaked := range []string{"alice@example.com", "123-45-6789", "eyJhbGciOi", "super-secret-password"} {
		if bytes.Contains(out, []byte(leaked)) {
			t.Fatalf("expected %q to be redacted, got %s", leaked, string(out))
		}
	}
	if !bytes.Contains(out, []byte("[REDACTED]")) {
		t.Fatalf("expected [REDACTED] marker in output: %s", string(out))
	}
}

func TestSequenceTrackerPartitionProducerIsolation(t *testing.T) {
	tracker := entity.NewSequenceTracker(100)
	now := time.Now().UTC()

	// Producer A has a gap (1, 3), while Producer B on the same tenant/partition is contiguous (1, 2).
	entries := []entity.ComplianceEntry{
		{TenantID: "t1", ProducerID: "prod-a", KafkaPartition: 2, SeqID: 1, EventTime: now},
		{TenantID: "t1", ProducerID: "prod-a", KafkaPartition: 2, SeqID: 3, EventTime: now},
		{TenantID: "t1", ProducerID: "prod-b", KafkaPartition: 2, SeqID: 1, EventTime: now},
		{TenantID: "t1", ProducerID: "prod-b", KafkaPartition: 2, SeqID: 2, EventTime: now},
	}

	manifests := tracker.TrackBatch(entries)
	if len(manifests) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(manifests))
	}

	var gapFound bool
	for _, m := range manifests {
		switch m.Key.ProducerID {
		case "prod-a":
			if len(m.Gaps) != 1 || m.Gaps[0].MinSeq != 2 || m.Gaps[0].MaxSeq != 2 {
				t.Fatalf("expected gap [2,2] for prod-a, got %+v", m.Gaps)
			}
			gapFound = true
		case "prod-b":
			if len(m.Gaps) != 0 {
				t.Fatalf("expected zero gaps for prod-b, got %+v", m.Gaps)
			}
		}
	}
	if !gapFound {
		t.Fatal("prod-a manifest not found")
	}
}

func TestAuditClientBackpressureAndDiagnosticShedding(t *testing.T) {
	prod := newBlockingProducer()
	diagSink := &countingDiagSink{}

	client, err := auditclient.NewClient(auditclient.Config{
		Producer:                prod,
		ProducerID:              "unit-sdk-01",
		NumPartitions:           8,
		WriteQueueSize:          1,
		ReadQueueSize:           10,
		WriteTimeout:            40 * time.Millisecond,
		DiagnosticBufferSize:    4,
		DiagnosticFlushInterval: time.Hour,
		DiagnosticSink:          diagSink,
		Logger:                  slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		close(prod.release)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Close(ctx)
	}()

	// 1. Diagnostic ring buffer drops oldest spans when capacity (4) is exceeded.
	for i := 0; i < 10; i++ {
		client.RecordDiagnostic(auditclient.DiagnosticSpan{TraceID: "tr", TenantID: "t1", Name: "span"})
	}
	if client.DroppedRecordsTotal() == 0 {
		t.Fatal("expected diagnostic ring buffer to record drops when overflowing")
	}

	// 2. Write-plane fails closed with ErrAuditUnavailable when worker & queue saturate.
	writeRec := &auditv1.AuditRecord{
		TenantId: "tenant-write",
		Plane:    auditv1.PlaneType_PLANE_TYPE_WRITE,
		Action:   "document.upload",
		Actor:    &auditv1.ActorContext{ActorId: "user-1"},
	}
	var gotUnavailable bool
	for i := 0; i < 5; i++ {
		err := client.RecordCompliance(context.Background(), writeRec)
		if errors.Is(err, auditclient.ErrAuditUnavailable) {
			gotUnavailable = true
			break
		}
	}
	if !gotUnavailable {
		t.Fatal("expected ErrAuditUnavailable when write-plane queue is saturated")
	}

	// 3. Read-plane sheds metadata at >= 90% fill without blocking.
	readRec := &auditv1.AuditRecord{
		TenantId: "tenant-read",
		Plane:    auditv1.PlaneType_PLANE_TYPE_READ,
		Action:   "search.query",
		Actor:    &auditv1.ActorContext{ActorId: "user-2"},
		AccessTrace: &auditv1.AccessTrace{
			DocIds:   []uint32{1, 2, 3},
			ChunkIds: []string{"c1", "c2"},
		},
		PayloadRedacted: []byte(strings.Repeat("x", 128)),
	}
	for i := 0; i < 20; i++ {
		if err := client.RecordCompliance(context.Background(), readRec); err != nil {
			t.Fatalf("read-plane should not block or fail: %v", err)
		}
	}
	if client.ShedMetadataTotal() == 0 {
		t.Fatal("expected read-plane metadata shedding at >= 90% queue utilization")
	}
}
