package chaos_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/twmb/franz-go/pkg/kgo"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/application/command"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/kafka"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/disk"
	"rag-telemetry-audit-daemon/pkg/auditclient"
	"rag-telemetry-audit-daemon/pkg/pool"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

type chaosHotStore struct {
	mu       sync.Mutex
	failErr  atomic.Pointer[error]
	written  map[uint64]entity.ComplianceEntry
	writeOps atomic.Int64
}

func newChaosHotStore() *chaosHotStore {
	return &chaosHotStore{written: make(map[uint64]entity.ComplianceEntry)}
}

func (s *chaosHotStore) setErr(err error) {
	if err == nil {
		s.failErr.Store(nil)
		return
	}
	s.failErr.Store(&err)
}

func (s *chaosHotStore) WriteBatch(_ context.Context, entries []entity.ComplianceEntry) error {
	s.writeOps.Add(1)
	if p := s.failErr.Load(); p != nil && *p != nil {
		return *p
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		s.written[entries[i].SeqID] = entries[i]
	}
	return nil
}

func (s *chaosHotStore) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return nil, nil
}

func (s *chaosHotStore) Ping(_ context.Context) error { return nil }
func (s *chaosHotStore) Close() error                 { return nil }

func (s *chaosHotStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

type coldRecordKey struct {
	tenant    string
	partition uint16
	seq       uint64
}

type chaosColdStore struct {
	mu          sync.Mutex
	failErr     atomic.Pointer[error]
	failPart    map[uint16]error
	written     map[coldRecordKey]entity.ComplianceEntry
	manifests   []entity.BatchManifest
	uploadCalls atomic.Int64
}

func newChaosColdStore() *chaosColdStore {
	return &chaosColdStore{
		failPart: make(map[uint16]error),
		written:  make(map[coldRecordKey]entity.ComplianceEntry),
	}
}

func (s *chaosColdStore) setErr(err error) {
	if err == nil {
		s.failErr.Store(nil)
		return
	}
	s.failErr.Store(&err)
}

func (s *chaosColdStore) setPartitionErr(partition uint16, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.failPart, partition)
	} else {
		s.failPart[partition] = err
	}
}

func (s *chaosColdStore) WriteBatch(_ context.Context, manifest entity.BatchManifest, entries []entity.ComplianceEntry) error {
	s.uploadCalls.Add(1)
	if p := s.failErr.Load(); p != nil && *p != nil {
		return *p
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.failPart[manifest.Key.KafkaPartition]; ok {
		return err
	}
	s.manifests = append(s.manifests, manifest)
	for i := range entries {
		k := coldRecordKey{tenant: entries[i].TenantID, partition: entries[i].KafkaPartition, seq: entries[i].SeqID}
		s.written[k] = entries[i]
	}
	return nil
}

func (s *chaosColdStore) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return nil, nil
}

func (s *chaosColdStore) Close() error { return nil }

func (s *chaosColdStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func makeRawKafkaBatch(tenant string, partition uint16, startSeq, count uint64) []command.RawKafkaRecord {
	ipHash := sha256.Sum256([]byte("10.0.0.1"))
	ipHex := hex.EncodeToString(ipHash[:])
	now := timestamppb.Now()

	out := make([]command.RawKafkaRecord, 0, count)
	for i := uint64(0); i < count; i++ {
		seq := startSeq + i
		pb := &auditv1.AuditRecord{
			EventId:     fmt.Sprintf("evt-%s-%d", tenant, seq),
			TraceId:     fmt.Sprintf("tr-%d", seq),
			TenantId:    tenant,
			ProducerId:  "chaos-prod-1",
			SeqId:       seq,
			EventTime:   now,
			Plane:       auditv1.PlaneType_PLANE_TYPE_WRITE,
			Action:      "document.upload",
			ServiceName: "rag-kb-catalog-service",
			Actor: &auditv1.ActorContext{
				ActorId:      "actor-1",
				TenantId:     tenant,
				AuthMethod:   "jwt",
				SourceIpHash: ipHex,
			},
			PayloadRedacted: []byte(`{"email":"ops@example.com","doc":"payload"}`),
		}
		raw, err := proto.Marshal(pb)
		if err != nil {
			panic(err)
		}
		out = append(out, command.RawKafkaRecord{
			Partition: partition,
			Offset:    seq - 1,
			Value:     raw,
		})
	}
	return out
}

func TestChaos_S3SlowDownAnd500SpilloverAndRecovery(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := observability.NewMetrics()

	wal, err := disk.NewMmapWAL(t.TempDir(), 64*1024*1024, 4*1024*1024, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hot := newChaosHotStore()
	cold := newChaosColdStore()
	cold.setErr(errors.New("s3 503 SlowDown: Please reduce your request rate"))

	hotFlusher := command.NewHotFlusher(hot, 50, time.Second)
	coldFlusher := command.NewColdFlusher(cold, 1024, time.Minute, 2)
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(bp), nil, hotFlusher, coldFlusher, hot, cold, wal, logger, metrics)

	// 1. Ingest 200 records while S3 returns 503 SlowDown, then 500 InternalError.
	if err := proc.Handle(ctx, makeRawKafkaBatch("tenant-s3-chaos", 0, 1, 100)); err == nil {
		t.Fatal("expected cold error during S3 503 SlowDown")
	}
	cold.setErr(errors.New("s3 500 InternalError"))
	if err := proc.Handle(ctx, makeRawKafkaBatch("tenant-s3-chaos", 0, 101, 100)); err == nil {
		t.Fatal("expected cold error during S3 500 InternalError")
	}
	_ = proc.FlushAll(ctx)

	if hot.count() != 200 {
		t.Fatalf("expected all 200 records in ClickHouse hot tier, got %d", hot.count())
	}
	if !wal.IsSpilloverActive() {
		t.Fatal("expected mmap_wal spillover to be active during S3 outage")
	}
	if cold.count() != 0 {
		t.Fatalf("expected 0 records in S3 during outage, got %d", cold.count())
	}

	// 2. Recover S3 and drain WAL.
	cold.setErr(nil)
	for wal.IsSpilloverActive() {
		if _, err := proc.DrainWALOnce(ctx, 64); err != nil {
			t.Fatalf("DrainWALOnce failed after S3 recovery: %v", err)
		}
	}

	if cold.count() != 200 {
		t.Fatalf("expected all 200 records replayed to S3 after recovery, got %d", cold.count())
	}
	if wal.ActiveBytes() != 0 {
		t.Fatalf("expected WAL active bytes == 0 after drain, got %d", wal.ActiveBytes())
	}
}

func TestChaos_ClickHouseFailoverHaltsWatermarkAndRecovers(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := observability.NewMetrics()

	wal, err := disk.NewMmapWAL(t.TempDir(), 64*1024*1024, 4*1024*1024, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hot := newChaosHotStore()
	cold := newChaosColdStore()
	hot.setErr(errors.New("clickhouse tcp connection refused"))

	hotFlusher := command.NewHotFlusher(hot, 50, time.Second)
	coldFlusher := command.NewColdFlusher(cold, 1024*1024, time.Minute, 2)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(nil), nil, hotFlusher, coldFlusher, hot, cold, wal, logger, metrics)

	if err := proc.Handle(ctx, makeRawKafkaBatch("tenant-ch-chaos", 1, 1, 150)); err == nil {
		t.Fatal("expected ClickHouse failure")
	}

	// Kafka offset watermarks must NOT advance for failed hot tier.
	if marks := hotFlusher.FlushedWatermarks(); len(marks) != 0 {
		t.Fatalf("expected zero flushed hot watermarks during ClickHouse outage, got %+v", marks)
	}
	if !wal.IsSpilloverActive() {
		t.Fatal("expected WAL spillover active during ClickHouse failover")
	}

	// ClickHouse comes back online -> drain WAL.
	hot.setErr(nil)
	for wal.IsSpilloverActive() {
		if _, err := proc.DrainWALOnce(ctx, 100); err != nil {
			t.Fatalf("DrainWALOnce failed after ClickHouse recovery: %v", err)
		}
	}
	if err := proc.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll after recovery: %v", err)
	}

	if hot.count() != 150 || cold.count() != 150 {
		t.Fatalf("expected 150 records in both tiers after recovery, got hot=%d cold=%d", hot.count(), cold.count())
	}
}

func TestChaos_PartitionFlushPartialFailureSafety(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	wal, err := disk.NewMmapWAL(t.TempDir(), 64*1024*1024, 4*1024*1024, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hot := newChaosHotStore()
	cold := newChaosColdStore()
	// Simulate partition 2 failing S3 upload during rebalance while partition 1 succeeds.
	cold.setPartitionErr(2, errors.New("broker rebalance / transient s3 timeout"))

	hotFlusher := command.NewHotFlusher(hot, 1000, time.Minute)
	coldFlusher := command.NewColdFlusher(cold, 100*1024*1024, time.Minute, 2)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(nil), nil, hotFlusher, coldFlusher, hot, cold, wal, logger, nil)

	var combined []command.RawKafkaRecord
	combined = append(combined, makeRawKafkaBatch("tenant-reb-1", 1, 1, 20)...)
	combined = append(combined, makeRawKafkaBatch("tenant-reb-2", 2, 1, 20)...)
	if err := proc.Handle(ctx, combined); err != nil {
		t.Fatalf("Handle buffered: %v", err)
	}

	// Rebalance revokes partition 1 (succeeds) and partition 2 (fails cold -> spills to WAL).
	if err := proc.FlushPartition(ctx, 1); err != nil {
		t.Fatalf("FlushPartition(1) unexpected error: %v", err)
	}
	if err := proc.FlushPartition(ctx, 2); err == nil {
		t.Fatal("expected FlushPartition(2) to fail cold flush")
	}

	coldMarks := coldFlusher.FlushedWatermarks()
	if _, ok := coldMarks[1]; !ok {
		t.Fatal("expected partition 1 cold watermark to advance")
	}
	if _, ok := coldMarks[2]; ok {
		t.Fatal("partition 2 cold watermark must NOT advance after failed flush")
	}

	// Clear error and drain WAL -> partition 2 recovers with zero lost records.
	cold.setPartitionErr(2, nil)
	for wal.IsSpilloverActive() {
		if _, err := proc.DrainWALOnce(ctx, 50); err != nil {
			t.Fatalf("DrainWALOnce: %v", err)
		}
	}
	if cold.count() != 40 {
		t.Fatalf("expected 40 cold records across both partitions, got %d", cold.count())
	}
}

func TestChaos_ConsumerGroupRebalanceCommitSafety(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	wal, err := disk.NewMmapWAL(t.TempDir(), 64*1024*1024, 4*1024*1024, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hot := newChaosHotStore()
	cold := newChaosColdStore()
	cold.setPartitionErr(2, errors.New("broker rebalance / transient s3 timeout"))

	hotFlusher := command.NewHotFlusher(hot, 1000, time.Minute)
	coldFlusher := command.NewColdFlusher(cold, 100*1024*1024, time.Minute, 2)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(nil), nil, hotFlusher, coldFlusher, hot, cold, wal, logger, nil)

	var commitMu sync.Mutex
	committed := make(map[int32]int64)
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Brokers:     []string{"127.0.0.1:9092"},
		Topic:       kafka.DefaultTopic,
		GroupID:     "chaos-rebalance-group",
		Processor:   proc,
		HotFlusher:  hotFlusher,
		ColdFlusher: coldFlusher,
		WAL:         wal,
		Logger:      logger,
		CommitFunc: func(_ context.Context, offsets map[string]map[int32]kgo.EpochOffset) error {
			commitMu.Lock()
			defer commitMu.Unlock()
			for _, parts := range offsets {
				for p, eo := range parts {
					if eo.Offset > committed[p] {
						committed[p] = eo.Offset
					}
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConsumerGroup: %v", err)
	}
	defer cg.Close()

	var combined []command.RawKafkaRecord
	combined = append(combined, makeRawKafkaBatch("tenant-reb-1", 1, 1, 20)...)
	combined = append(combined, makeRawKafkaBatch("tenant-reb-2", 2, 1, 20)...)
	if err := proc.Handle(ctx, combined); err != nil {
		t.Fatalf("Handle buffered: %v", err)
	}

	// Revoke partitions 1 and 2: partition 1 flushes cleanly, partition 2 fails cold flush -> spills to WAL.
	cg.RevokePartitions(ctx, []int32{1, 2})

	commitMu.Lock()
	p2BeforeRecovery := committed[2]
	commitMu.Unlock()
	if p2BeforeRecovery != 0 {
		t.Fatalf("partition 2 offset must NOT commit while unresolved in WAL, got %d", p2BeforeRecovery)
	}

	// Recover S3, drain WAL, process a post-recovery record on partition 2, and commit safe offsets.
	cold.setPartitionErr(2, nil)
	for wal.IsSpilloverActive() {
		if _, err := proc.DrainWALOnce(ctx, 50); err != nil {
			t.Fatalf("DrainWALOnce: %v", err)
		}
	}
	if cold.count() != 40 {
		t.Fatalf("expected 40 cold records after WAL drain, got %d", cold.count())
	}
	if err := proc.Handle(ctx, makeRawKafkaBatch("tenant-reb-2", 2, 21, 1)); err != nil {
		t.Fatalf("Handle post-recovery: %v", err)
	}
	if err := proc.FlushPartition(ctx, 2); err != nil {
		t.Fatalf("FlushPartition(2) post-recovery: %v", err)
	}
	cg.CommitSafeOffsets(ctx)

	commitMu.Lock()
	p1After := committed[1]
	p2After := committed[2]
	commitMu.Unlock()
	if p1After != 20 || p2After != 21 {
		t.Fatalf("expected committed offsets p1=20 p2=21 after recovery, got p1=%d p2=%d", p1After, p2After)
	}
}

func TestChaos_WALCapacitySaturationAndCallerFailClosed(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Tiny 4 KiB WAL cap to trigger ErrWALFull rapidly.
	wal, err := disk.NewMmapWAL(t.TempDir(), 4096, 2048, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	hot := newChaosHotStore()
	cold := newChaosColdStore()
	hot.setErr(errors.New("clickhouse down"))

	hotFlusher := command.NewHotFlusher(hot, 5, time.Second)
	coldFlusher := command.NewColdFlusher(cold, 1024*1024, time.Minute, 1)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(nil), nil, hotFlusher, coldFlusher, hot, cold, wal, logger, nil)

	var gotWALFull bool
	for seq := uint64(1); seq <= 100; seq += 5 {
		err := proc.Handle(ctx, makeRawKafkaBatch("tenant-wal-full", 0, seq, 5))
		if errors.Is(err, repository.ErrWALFull) {
			gotWALFull = true
			break
		}
	}
	if !gotWALFull {
		t.Fatal("expected repository.ErrWALFull when WAL capacity ceiling is reached")
	}

	// Verify caller SDK fails closed on write-plane when Kafka producer errors.
	client, err := auditclient.NewClient(auditclient.Config{
		Producer:       errProducer{err: errors.New("kafka broker down")},
		ProducerID:     "chaos-sdk",
		WriteQueueSize: 2,
		ReadQueueSize:  8,
		WriteTimeout:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	err = client.RecordCompliance(ctx, &auditv1.AuditRecord{
		TenantId:        "tenant-wal-full",
		Plane:           auditv1.PlaneType_PLANE_TYPE_WRITE,
		Action:          "acl.mutate",
		Actor:           &auditv1.ActorContext{ActorId: "admin"},
		PayloadRedacted: []byte(strings.Repeat("a", 64)),
	})
	if !errors.Is(err, auditclient.ErrAuditUnavailable) {
		t.Fatalf("expected ErrAuditUnavailable on write-plane when Kafka is down, got %v", err)
	}
}

type errProducer struct{ err error }

func (p errProducer) ProduceSync(_ context.Context, _ string, _ int32, _, _ []byte) error {
	return p.err
}
func (p errProducer) Close() {}
