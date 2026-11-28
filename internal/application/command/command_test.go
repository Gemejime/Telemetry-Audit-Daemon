package command

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/disk"
)

type stubHotStore struct {
	mu      sync.Mutex
	batches [][]entity.ComplianceEntry
	fail    bool
}

func (s *stubHotStore) WriteBatch(_ context.Context, entries []entity.ComplianceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("clickhouse down")
	}
	cp := make([]entity.ComplianceEntry, len(entries))
	copy(cp, entries)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *stubHotStore) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return nil, nil
}
func (s *stubHotStore) Ping(_ context.Context) error { return nil }
func (s *stubHotStore) Close() error                 { return nil }

type stubColdStore struct {
	mu        sync.Mutex
	manifests []entity.BatchManifest
	entries   [][]entity.ComplianceEntry
	fail      bool
}

func (s *stubColdStore) WriteBatch(_ context.Context, manifest entity.BatchManifest, entries []entity.ComplianceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("s3 503 slowdown")
	}
	s.manifests = append(s.manifests, manifest)
	cp := make([]entity.ComplianceEntry, len(entries))
	copy(cp, entries)
	s.entries = append(s.entries, cp)
	return nil
}

func (s *stubColdStore) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return nil, nil
}
func (s *stubColdStore) Close() error { return nil }

func makeAuditProtoBytes(t *testing.T, tenantID, producerID string, seq uint64, payload string) []byte {
	t.Helper()
	rec := &auditv1.AuditRecord{
		EventId:     "evt-" + producerID,
		TraceId:     "trc-1",
		TenantId:    tenantID,
		ProducerId:  producerID,
		SeqId:       seq,
		EventTime:   timestamppb.New(time.Unix(1700000000, 0).UTC()),
		Plane:       auditv1.PlaneType_PLANE_TYPE_WRITE,
		Action:      "document.upload",
		ServiceName: "rag-kb-catalog-service",
		Actor: &auditv1.ActorContext{
			ActorId:      "user-1",
			TenantId:     tenantID,
			AuthMethod:   "oidc",
			SourceIpHash: strings.Repeat("a", 64),
		},
		PayloadRedacted: []byte(payload),
	}
	b, err := proto.Marshal(rec)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return b
}

func TestProcessKafkaBatch_RedactsPoisonPillsAndFlushes(t *testing.T) {
	hot := &stubHotStore{}
	cold := &stubColdStore{}
	metrics := observability.NewMetrics()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	hotFlusher := NewHotFlusher(hot, 10, time.Second)
	coldFlusher := NewColdFlusher(cold, 1024*1024, time.Minute, 2)
	proc := NewProcessKafkaBatch(nil, nil, hotFlusher, coldFlusher, hot, cold, nil, logger, metrics)

	records := []RawKafkaRecord{
		{Partition: 1, Offset: 10, Value: []byte("not-valid-protobuf")},
		{Partition: 1, Offset: 11, Value: makeAuditProtoBytes(t, "tenant-a", "prod-1", 1, `{"action":"search","email":"alice@example.com"}`)},
		{Partition: 1, Offset: 12, Value: makeAuditProtoBytes(t, "tenant-a", "prod-2", 5, `{"doc_id":"doc-42","ssn":"123-45-6789"}`)},
	}

	if err := proc.Handle(context.Background(), records); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := proc.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if len(hot.batches) != 1 || len(hot.batches[0]) != 2 {
		t.Fatalf("expected 2 valid records in ClickHouse, got %+v", hot.batches)
	}
	byOffset := make(map[uint64]entity.ComplianceEntry, len(hot.batches[0]))
	for _, entry := range hot.batches[0] {
		byOffset[entry.KafkaOffset] = entry
	}
	emailEntry, ok := byOffset[11]
	if !ok {
		t.Fatalf("missing offset 11 entry in ClickHouse batch: %+v", hot.batches[0])
	}
	emailPayload := string(emailEntry.PayloadRedacted)
	if strings.Contains(emailPayload, "alice@example.com") || !strings.Contains(emailPayload, `"action":"search"`) {
		t.Fatalf("expected email redacted and non-sensitive field preserved, got %s", emailPayload)
	}
	ssnEntry, ok := byOffset[12]
	if !ok {
		t.Fatalf("missing offset 12 entry in ClickHouse batch: %+v", hot.batches[0])
	}
	ssnPayload := string(ssnEntry.PayloadRedacted)
	if strings.Contains(ssnPayload, "123-45-6789") || !strings.Contains(ssnPayload, `"doc_id":"doc-42"`) {
		t.Fatalf("expected SSN redacted and non-sensitive field preserved, got %s", ssnPayload)
	}
	if len(cold.manifests) != 2 {
		t.Fatalf("expected 2 per-producer S3 Parquet manifests, got %d", len(cold.manifests))
	}
	if hotFlusher.FlushedWatermarks()[1] != 12 || coldFlusher.FlushedWatermarks()[1] != 12 {
		t.Fatalf("expected watermarks at offset 12, got hot=%v cold=%v", hotFlusher.FlushedWatermarks(), coldFlusher.FlushedWatermarks())
	}
}

func TestProcessKafkaBatch_EnforcesProducerTenantBinding(t *testing.T) {
	hot := &stubHotStore{}
	cold := &stubColdStore{}
	metrics := observability.NewMetrics()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	hotFlusher := NewHotFlusher(hot, 10, time.Second)
	coldFlusher := NewColdFlusher(cold, 1024*1024, time.Minute, 2)
	proc := NewProcessKafkaBatch(nil, nil, hotFlusher, coldFlusher, hot, cold, nil, logger, metrics)
	proc.SetProducerTenantBindings(map[string]string{
		"prod-trusted": "tenant-a",
	})

	records := []RawKafkaRecord{
		{Partition: 0, Offset: 1, Value: makeAuditProtoBytes(t, "tenant-b", "prod-trusted", 1, `{"ok":false}`)},
		{Partition: 0, Offset: 2, Value: makeAuditProtoBytes(t, "tenant-a", "prod-unknown", 1, `{"ok":false}`)},
		{Partition: 0, Offset: 3, Value: makeAuditProtoBytes(t, "tenant-a", "prod-trusted", 1, `{"ok":true}`)},
	}

	if err := proc.Handle(context.Background(), records); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := proc.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if len(hot.batches) != 1 || len(hot.batches[0]) != 1 {
		t.Fatalf("expected only 1 trusted entry in hot store, got %+v", hot.batches)
	}
	if len(cold.entries) != 1 || len(cold.entries[0]) != 1 {
		t.Fatalf("expected only 1 trusted entry in cold store, got %+v", cold.entries)
	}
}

func TestProcessKafkaBatch_SpillsToWALAndRecovers(t *testing.T) {
	hot := &stubHotStore{fail: true}
	cold := &stubColdStore{}
	metrics := observability.NewMetrics()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	wal, err := disk.NewMmapWAL(t.TempDir(), 1024*1024, 64*1024, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hotFlusher := NewHotFlusher(hot, 1, time.Second)
	coldFlusher := NewColdFlusher(cold, 1024*1024, time.Minute, 2)
	proc := NewProcessKafkaBatch(nil, nil, hotFlusher, coldFlusher, hot, cold, wal, logger, metrics)

	records := []RawKafkaRecord{
		{Partition: 2, Offset: 50, Value: makeAuditProtoBytes(t, "tenant-b", "prod-1", 1, `{"ok":true}`)},
	}

	err = proc.Handle(context.Background(), records)
	if err == nil {
		t.Fatal("expected error from failing ClickHouse")
	}
	if !wal.IsSpilloverActive() {
		t.Fatal("expected WAL spillover to be active after ClickHouse failure")
	}
	if _, ok := hotFlusher.FlushedWatermarks()[2]; ok {
		t.Fatal("hot watermark must not advance on failed ClickHouse write")
	}

	// Recover ClickHouse and drain WAL.
	hot.mu.Lock()
	hot.fail = false
	hot.mu.Unlock()

	drained, err := proc.DrainWALOnce(context.Background(), 100)
	if err != nil {
		t.Fatalf("DrainWALOnce: %v", err)
	}
	if drained != 1 {
		t.Fatalf("expected 1 drained WAL record, got %d", drained)
	}
	if wal.IsSpilloverActive() {
		t.Fatal("expected WAL spillover to be inactive after DrainWALOnce")
	}
	if len(hot.batches) != 1 || len(hot.batches[0]) != 1 {
		t.Fatalf("expected 1 replayed record in ClickHouse, got %+v", hot.batches)
	}
}
