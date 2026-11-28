package entity

import (
	"testing"
	"time"
)

const validHash64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func makeEntry(producer, tenant string, part uint16, seq uint64) ComplianceEntry {
	return ComplianceEntry{
		EventID:        "evt-1",
		TraceID:        "tr-1",
		TenantID:       tenant,
		ProducerID:     producer,
		KafkaPartition: part,
		KafkaOffset:    seq,
		SeqID:          seq,
		EventTime:      time.Now().UTC(),
		Plane:          PlaneRead,
		Action:         "search.query",
		ActorID:        "actor-1",
		SourceIPHash:   validHash64,
	}
}

func TestComplianceEntry_Validate(t *testing.T) {
	valid := makeEntry("prod-a", "tenant-1", 0, 1)
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid entry, got %v", err)
	}

	badHash := valid
	badHash.SourceIPHash = "short"
	if err := badHash.Validate(); err == nil {
		t.Fatal("expected error for invalid source_ip_hash")
	}

	zeroSeq := valid
	zeroSeq.SeqID = 0
	if err := zeroSeq.Validate(); err == nil {
		t.Fatal("expected error for zero seq_id")
	}
}

func TestSequenceTracker_ContinuityGapsAndDuplicates(t *testing.T) {
	tracker := NewSequenceTracker(10)

	// Batch 1: contiguous 1, 2, 3
	b1 := []ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 1),
		makeEntry("prod-a", "tenant-1", 0, 2),
		makeEntry("prod-a", "tenant-1", 0, 3),
	}
	m1 := tracker.TrackBatch(b1)
	if len(m1) != 1 || len(m1[0].Gaps) != 0 || m1[0].MinSeq != 1 || m1[0].MaxSeq != 3 {
		t.Fatalf("unexpected batch 1 manifest: %+v", m1)
	}

	// Batch 2: cross-batch gap (missing 4), intra-batch gap (missing 7), out-of-order (6 before 5), duplicate (2, 5)
	b2 := []ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 6),
		makeEntry("prod-a", "tenant-1", 0, 5),
		makeEntry("prod-a", "tenant-1", 0, 5),
		makeEntry("prod-a", "tenant-1", 0, 2),
		makeEntry("prod-a", "tenant-1", 0, 8),
	}
	m2 := tracker.TrackBatch(b2)
	if len(m2) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(m2))
	}
	got := m2[0]
	if got.DuplicateCount != 2 {
		t.Fatalf("expected 2 duplicates, got %d", got.DuplicateCount)
	}
	if got.OutOfOrderCount == 0 {
		t.Fatal("expected out-of-order count > 0")
	}
	if len(got.Gaps) != 2 {
		t.Fatalf("expected 2 gaps (4 and 7), got %+v", got.Gaps)
	}
	if got.Gaps[0].MinSeq != 4 || got.Gaps[0].MaxSeq != 4 {
		t.Fatalf("expected first gap [4,4], got %+v", got.Gaps[0])
	}
	if got.Gaps[1].MinSeq != 7 || got.Gaps[1].MaxSeq != 7 {
		t.Fatalf("expected second gap [7,7], got %+v", got.Gaps[1])
	}
}

func TestSequenceTracker_BoundedEvictionAndProducerIsolation(t *testing.T) {
	tracker := NewSequenceTracker(2)

	tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 10),
		makeEntry("prod-b", "tenant-1", 0, 1),
	})
	if tracker.TrackedStreams() != 2 {
		t.Fatalf("expected 2 tracked streams, got %d", tracker.TrackedStreams())
	}

	// Adding a 3rd stream evicts the oldest ("prod-a").
	tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-c", "tenant-1", 0, 1),
	})
	if tracker.TrackedStreams() != 2 {
		t.Fatalf("expected 2 tracked streams after eviction, got %d", tracker.TrackedStreams())
	}
}

func TestSequenceTracker_CrossBatchGapAndLateGapFill(t *testing.T) {
	tracker := NewSequenceTracker(10)

	// Batch 1: [1, 2, 3]
	m1 := tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 1),
		makeEntry("prod-a", "tenant-1", 0, 2),
		makeEntry("prod-a", "tenant-1", 0, 3),
	})
	if len(m1[0].Gaps) != 0 {
		t.Fatalf("expected no gaps in batch 1, got %+v", m1[0].Gaps)
	}

	// Batch 2: [6, 7] -> cross-batch gap [4, 5] reported once
	m2 := tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 6),
		makeEntry("prod-a", "tenant-1", 0, 7),
	})
	if len(m2[0].Gaps) != 1 || m2[0].Gaps[0].MinSeq != 4 || m2[0].Gaps[0].MaxSeq != 5 {
		t.Fatalf("expected single gap [4,5], got %+v", m2[0].Gaps)
	}

	// Batch 3: late gap fill [4, 5] must not be counted as duplicates
	m3 := tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 4),
		makeEntry("prod-a", "tenant-1", 0, 5),
	})
	if m3[0].DuplicateCount != 0 {
		t.Fatalf("expected 0 duplicates for late gap fill, got %d", m3[0].DuplicateCount)
	}

	// Batch 4: replay of [4] is now a duplicate
	m4 := tracker.TrackBatch([]ComplianceEntry{
		makeEntry("prod-a", "tenant-1", 0, 4),
	})
	if m4[0].DuplicateCount != 1 {
		t.Fatalf("expected 1 duplicate for replayed gap fill, got %d", m4[0].DuplicateCount)
	}
}
