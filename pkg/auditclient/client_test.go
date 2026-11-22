package auditclient

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
)

type recordedProduce struct {
	topic     string
	partition int32
	key       []byte
	record    *auditv1.AuditRecord
}

type mockProducer struct {
	mu      sync.Mutex
	records []recordedProduce
	err     error
	blockCh chan struct{}
}

func (m *mockProducer) ProduceSync(ctx context.Context, topic string, partition int32, key, value []byte) error {
	if m.blockCh != nil {
		select {
		case <-m.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	var rec auditv1.AuditRecord
	if err := proto.Unmarshal(value, &rec); err != nil {
		return err
	}
	m.records = append(m.records, recordedProduce{
		topic:     topic,
		partition: partition,
		key:       append([]byte(nil), key...),
		record:    &rec,
	})
	return nil
}

func (m *mockProducer) Close() {}

func (m *mockProducer) snapshot() []recordedProduce {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]recordedProduce, len(m.records))
	copy(out, m.records)
	return out
}

type mockDiagnosticSink struct {
	mu    sync.Mutex
	spans []DiagnosticSpan
}

func (s *mockDiagnosticSink) Export(_ context.Context, spans []DiagnosticSpan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, spans...)
	return nil
}

func (s *mockDiagnosticSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spans)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func validWriteRecord(tenantID string) *auditv1.AuditRecord {
	return &auditv1.AuditRecord{
		TraceId:     "trace-123",
		TenantId:    tenantID,
		Plane:       auditv1.PlaneType_PLANE_TYPE_WRITE,
		Action:      "document.upload",
		ServiceName: "rag-kb-catalog-service",
		Actor: &auditv1.ActorContext{
			ActorId:      "user-42",
			TenantId:     tenantID,
			AuthMethod:   "jwt",
			SourceIpHash: "hash-abc",
		},
		PayloadRedacted: []byte(`{"doc":"redacted"}`),
	}
}

func validReadRecord(tenantID string) *auditv1.AuditRecord {
	return &auditv1.AuditRecord{
		TraceId:     "trace-read-1",
		TenantId:    tenantID,
		Plane:       auditv1.PlaneType_PLANE_TYPE_READ,
		Action:      "search.query",
		ServiceName: "rag-search-orchestrator",
		Actor: &auditv1.ActorContext{
			ActorId:      "user-99",
			TenantId:     tenantID,
			AuthMethod:   "api_key",
			SourceIpHash: "hash-xyz",
		},
		AccessTrace: &auditv1.AccessTrace{
			DocIds:                []uint32{101, 202},
			ChunkIds:              []string{"c1", "c2"},
			CacheHit:              true,
			EmbeddingModelVersion: "text-embedding-3-large",
			DocumentVersions:      []string{"v1", "v2"},
		},
		PayloadRedacted:  []byte(`{"query":"[REDACTED]"}`),
		PromptTokens:     120,
		CompletionTokens: 45,
	}
}

func TestRecordComplianceValidation(t *testing.T) {
	mp := &mockProducer{}
	cl, err := NewClient(Config{
		Producer:   mp,
		ProducerID: "pod-test-1",
		Logger:     discardLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cl.Close(context.Background()) }()

	cases := []struct {
		name string
		rec  *auditv1.AuditRecord
	}{
		{"nil record", nil},
		{"missing tenant_id", &auditv1.AuditRecord{Plane: auditv1.PlaneType_PLANE_TYPE_WRITE, Action: "a", Actor: &auditv1.ActorContext{ActorId: "u"}}},
		{"unspecified plane", &auditv1.AuditRecord{TenantId: "t1", Plane: auditv1.PlaneType_PLANE_TYPE_UNSPECIFIED, Action: "a", Actor: &auditv1.ActorContext{ActorId: "u"}}},
		{"missing action", &auditv1.AuditRecord{TenantId: "t1", Plane: auditv1.PlaneType_PLANE_TYPE_WRITE, Actor: &auditv1.ActorContext{ActorId: "u"}}},
		{"missing actor", &auditv1.AuditRecord{TenantId: "t1", Plane: auditv1.PlaneType_PLANE_TYPE_WRITE, Action: "a"}},
		{"missing actor_id", &auditv1.AuditRecord{TenantId: "t1", Plane: auditv1.PlaneType_PLANE_TYPE_WRITE, Action: "a", Actor: &auditv1.ActorContext{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := cl.RecordCompliance(context.Background(), tc.rec)
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("expected ErrInvalidRecord, got %v", err)
			}
		})
	}
}

func TestPartitionDeterminismAndMonotonicSeq(t *testing.T) {
	mp := &mockProducer{}
	cl, err := NewClient(Config{
		Producer:      mp,
		ProducerID:    "pod-seq-1",
		NumPartitions: 8,
		Logger:        discardLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	p1 := PartitionForTenant("tenant-alpha", 8)
	p2 := PartitionForTenant("tenant-alpha", 8)
	if p1 != p2 || p1 < 0 || p1 >= 8 {
		t.Fatalf("non-deterministic or out-of-range partition: p1=%d p2=%d", p1, p2)
	}

	// Failed validation must not increment seq_id.
	_ = cl.RecordCompliance(context.Background(), &auditv1.AuditRecord{TenantId: "tenant-alpha"})

	for i := 0; i < 3; i++ {
		if err := cl.RecordCompliance(context.Background(), validWriteRecord("tenant-alpha")); err != nil {
			t.Fatalf("RecordCompliance %d: %v", i, err)
		}
	}
	if err := cl.RecordCompliance(context.Background(), validWriteRecord("tenant-beta")); err != nil {
		t.Fatalf("RecordCompliance beta: %v", err)
	}

	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	recs := mp.snapshot()
	if len(recs) != 4 {
		t.Fatalf("expected 4 records, got %d", len(recs))
	}

	for i := 0; i < 3; i++ {
		r := recs[i].record
		if r.SeqId != uint64(i+1) {
			t.Fatalf("tenant-alpha record %d: expected seq_id %d, got %d", i, i+1, r.SeqId)
		}
		if r.EventId == "" || r.EventTime == nil || r.ProducerId != "pod-seq-1" {
			t.Fatalf("missing enrichment on record %d: %+v", i, r)
		}
		if recs[i].partition != p1 {
			t.Fatalf("expected partition %d, got %d", p1, recs[i].partition)
		}
	}
	if recs[3].record.SeqId != 1 {
		t.Fatalf("tenant-beta expected independent seq_id 1, got %d", recs[3].record.SeqId)
	}
}

func TestWritePlaneFailClosedBackpressure(t *testing.T) {
	t.Run("broker error returns ErrAuditUnavailable", func(t *testing.T) {
		mp := &mockProducer{err: errors.New("broker down")}
		cl, err := NewClient(Config{
			Producer:     mp,
			ProducerID:   "pod-err",
			WriteTimeout: 200 * time.Millisecond,
			Logger:       discardLogger(),
		})
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer func() { _ = cl.Close(context.Background()) }()

		err = cl.RecordCompliance(context.Background(), validWriteRecord("tenant-1"))
		if !errors.Is(err, ErrAuditUnavailable) {
			t.Fatalf("expected ErrAuditUnavailable, got %v", err)
		}
	})

	t.Run("full write queue times out with ErrAuditUnavailable", func(t *testing.T) {
		blockCh := make(chan struct{})
		mp := &mockProducer{blockCh: blockCh}
		cl, err := NewClient(Config{
			Producer:       mp,
			ProducerID:     "pod-full",
			WriteQueueSize: 1,
			WriteTimeout:   40 * time.Millisecond,
			Logger:         discardLogger(),
		})
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer func() {
			close(blockCh)
			_ = cl.Close(context.Background())
		}()

		// First write enters worker and blocks in ProduceSync.
		go func() {
			_ = cl.RecordCompliance(context.Background(), validWriteRecord("tenant-1"))
		}()
		time.Sleep(10 * time.Millisecond)

		// Second write fills the 1-slot writeQueue and blocks waiting for ack.
		go func() {
			_ = cl.RecordCompliance(context.Background(), validWriteRecord("tenant-1"))
		}()
		time.Sleep(10 * time.Millisecond)

		// Third write must fail closed with ErrAuditUnavailable.
		err = cl.RecordCompliance(context.Background(), validWriteRecord("tenant-1"))
		if !errors.Is(err, ErrAuditUnavailable) {
			t.Fatalf("expected ErrAuditUnavailable on full queue, got %v", err)
		}
	})
}

func TestReadPlaneSheddingAndDropWithoutStarvingWritePlane(t *testing.T) {
	blockCh := make(chan struct{})
	mp := &mockProducer{blockCh: blockCh}
	cl, err := NewClient(Config{
		Producer:       mp,
		ProducerID:     "pod-shed",
		ReadQueueSize:  10,
		WriteQueueSize: 4,
		WriteTimeout:   200 * time.Millisecond,
		Logger:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Fill read queue to 100% + overflow while producer is blocked.
	for i := 0; i < 20; i++ {
		if err := cl.RecordCompliance(context.Background(), validReadRecord("tenant-read")); err != nil {
			t.Fatalf("read plane must be non-blocking, got err: %v", err)
		}
	}

	if cl.ShedMetadataTotal() == 0 {
		t.Fatalf("expected >= 90%% fill metadata shedding to trigger")
	}
	if cl.DroppedRecordsTotal() == 0 {
		t.Fatalf("expected 100%% fill read-plane drops to increment DroppedRecordsTotal")
	}

	// Unblock producer and verify write-plane still succeeds cleanly.
	close(blockCh)
	if err := cl.RecordCompliance(context.Background(), validWriteRecord("tenant-write")); err != nil {
		t.Fatalf("write plane should not be starved by read plane: %v", err)
	}

	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	recs := mp.snapshot()
	var sawShed bool
	for _, r := range recs {
		if r.record.Plane == auditv1.PlaneType_PLANE_TYPE_READ && len(r.record.PayloadRedacted) == 0 {
			sawShed = true
			if r.record.EventId == "" || r.record.TenantId == "" || r.record.Action == "" || r.record.Actor == nil {
				t.Fatalf("principal access fields must be preserved when shedding: %+v", r.record)
			}
			if r.record.AccessTrace == nil || len(r.record.AccessTrace.DocIds) == 0 {
				t.Fatalf("access_trace.doc_ids should be preserved when shedding")
			}
			if len(r.record.AccessTrace.ChunkIds) != 0 || len(r.record.AccessTrace.DocumentVersions) != 0 {
				t.Fatalf("non-essential access_trace fields should be shed")
			}
		}
	}
	if !sawShed {
		t.Fatalf("expected at least one shed read-plane record in published output")
	}
}

func TestDiagnosticRingBufferDropsAndExports(t *testing.T) {
	sink := &mockDiagnosticSink{}
	rb := NewRingBuffer(3, 5*time.Second, sink, discardLogger())

	for i := 0; i < 5; i++ {
		rb.Push(DiagnosticSpan{
			TraceID: "t",
			Name:    "span",
		})
	}

	if rb.Len() != 3 {
		t.Fatalf("expected buffer len 3, got %d", rb.Len())
	}
	if rb.DroppedTotal() != 2 {
		t.Fatalf("expected 2 dropped spans, got %d", rb.DroppedTotal())
	}

	if err := rb.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sink.count() != 3 {
		t.Fatalf("expected 3 exported spans on close, got %d", sink.count())
	}
}

func TestRingBufferNilSinkCountsDroppedOnFlush(t *testing.T) {
	rb := NewRingBuffer(4, time.Minute, nil, discardLogger())
	rb.Push(DiagnosticSpan{TraceID: "t1", Name: "span1"})
	rb.Push(DiagnosticSpan{TraceID: "t2", Name: "span2"})

	if err := rb.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if rb.DroppedTotal() != 2 {
		t.Fatalf("expected 2 dropped spans with nil sink, got %d", rb.DroppedTotal())
	}
}

func TestClientCloseRetriesAfterDeadlineExceeded(t *testing.T) {
	blockCh := make(chan struct{})
	mp := &mockProducer{blockCh: blockCh}
	sink := &mockDiagnosticSink{}
	cl, err := NewClient(Config{
		Producer:                mp,
		ProducerID:              "pod-close-retry",
		DiagnosticSink:          sink,
		DiagnosticFlushInterval: time.Minute,
		Logger:                  discardLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cl.RecordDiagnostic(DiagnosticSpan{TraceID: "t-diag", Name: "span"})

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cl.Close(expiredCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on first Close, got %v", err)
	}

	close(blockCh)
	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("expected second Close to succeed, got %v", err)
	}
	if sink.count() != 1 {
		t.Fatalf("expected diagnostic final flush to export 1 span, got %d", sink.count())
	}
}

func BenchmarkPartitionForTenant(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = PartitionForTenant("tenant-enterprise-001", 8)
	}
}

func BenchmarkRingBufferPush(b *testing.B) {
	rb := NewRingBuffer(4096, time.Minute, nil, discardLogger())
	defer func() { _ = rb.Close(context.Background()) }()
	span := DiagnosticSpan{TraceID: "trace-1", TenantID: "tenant-1", Name: "vector.search"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(span)
	}
}
