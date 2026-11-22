package entity

import (
	"container/list"
	"sort"
	"sync"
	"time"
)

// DefaultMaxTrackedStreams caps per-stream sequence tracking state to protect
// the daemon's 1.5 GiB RSS ceiling under high-cardinality producer churn.
const DefaultMaxTrackedStreams = 100_000

// StreamKey scopes monotonic sequence tracking to (producer_id, tenant_id, kafka_partition).
type StreamKey struct {
	ProducerID     string
	TenantID       string
	KafkaPartition uint16
}

// SequenceGap represents a missing contiguous sequence range [MinSeq, MaxSeq].
type SequenceGap struct {
	Key    StreamKey
	MinSeq uint64
	MaxSeq uint64
}

// BatchManifest summarizes sequence continuity for a single StreamKey within a batch.
type BatchManifest struct {
	Key             StreamKey
	MinSeq          uint64
	MaxSeq          uint64
	RecordCount     int
	Gaps            []SequenceGap
	DuplicateCount  int
	OutOfOrderCount int
}

type streamState struct {
	key             StreamKey
	expectedNextSeq uint64
	lastSeen        time.Time
	elem            *list.Element
}

// SequenceTracker maintains bounded LRU state per StreamKey to detect intra-batch
// and cross-batch sequence gaps, out-of-order arrivals, and duplicate replays.
type SequenceTracker struct {
	mu         sync.Mutex
	maxStreams int
	states     map[StreamKey]*streamState
	lru        *list.List // front = most recently used, back = eviction candidate
}

// NewSequenceTracker constructs a SequenceTracker with a bounded stream table.
func NewSequenceTracker(maxStreams int) *SequenceTracker {
	if maxStreams <= 0 {
		maxStreams = DefaultMaxTrackedStreams
	}
	return &SequenceTracker{
		maxStreams: maxStreams,
		states:     make(map[StreamKey]*streamState),
		lru:        list.New(),
	}
}

// TrackBatch analyzes entries grouped by StreamKey, returning one BatchManifest
// per distinct stream with detected intra-batch and cross-batch gaps.
func (t *SequenceTracker) TrackBatch(entries []ComplianceEntry) []BatchManifest {
	if len(entries) == 0 {
		return nil
	}

	grouped := make(map[StreamKey][]uint64)
	order := make([]StreamKey, 0, 8)
	for i := range entries {
		k := StreamKey{
			ProducerID:     entries[i].ProducerID,
			TenantID:       entries[i].TenantID,
			KafkaPartition: entries[i].KafkaPartition,
		}
		if _, exists := grouped[k]; !exists {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], entries[i].SeqID)
	}

	now := time.Now().UTC()
	manifests := make([]BatchManifest, 0, len(order))

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, key := range order {
		seqs := grouped[key]
		manifest := t.trackStreamLocked(key, seqs, now)
		manifests = append(manifests, manifest)
	}
	return manifests
}

func (t *SequenceTracker) trackStreamLocked(key StreamKey, seqs []uint64, now time.Time) BatchManifest {
	outOfOrder := 0
	for i := 1; i < len(seqs); i++ {
		if seqs[i] < seqs[i-1] {
			outOfOrder++
		}
	}

	sorted := make([]uint64, len(seqs))
	copy(sorted, seqs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	minSeq := sorted[0]
	maxSeq := sorted[len(sorted)-1]

	manifest := BatchManifest{
		Key:             key,
		MinSeq:          minSeq,
		MaxSeq:          maxSeq,
		RecordCount:     len(seqs),
		OutOfOrderCount: outOfOrder,
	}

	st, exists := t.states[key]
	if exists {
		t.lru.MoveToFront(st.elem)
		st.lastSeen = now
		if minSeq > st.expectedNextSeq {
			manifest.Gaps = append(manifest.Gaps, SequenceGap{
				Key:    key,
				MinSeq: st.expectedNextSeq,
				MaxSeq: minSeq - 1,
			})
		}
	} else {
		t.evictIfFullLocked()
		st = &streamState{
			key:             key,
			expectedNextSeq: minSeq,
			lastSeen:        now,
		}
		st.elem = t.lru.PushFront(st)
		t.states[key] = st
	}

	// Count cross-batch and intra-batch duplicates, and detect intra-batch gaps.
	var prev uint64
	hasPrev := false
	if exists && st.expectedNextSeq > 0 {
		prev = st.expectedNextSeq - 1
		hasPrev = true
	}
	for _, s := range sorted {
		if exists && s < st.expectedNextSeq {
			manifest.DuplicateCount++
			continue
		}
		if hasPrev {
			if s == prev {
				manifest.DuplicateCount++
				continue
			}
			if s > prev+1 {
				manifest.Gaps = append(manifest.Gaps, SequenceGap{
					Key:    key,
					MinSeq: prev + 1,
					MaxSeq: s - 1,
				})
			}
		}
		prev = s
		hasPrev = true
	}

	if maxSeq+1 > st.expectedNextSeq {
		st.expectedNextSeq = maxSeq + 1
	}

	return manifest
}

func (t *SequenceTracker) evictIfFullLocked() {
	for len(t.states) >= t.maxStreams {
		back := t.lru.Back()
		if back == nil {
			return
		}
		victim := back.Value.(*streamState)
		t.lru.Remove(back)
		delete(t.states, victim.key)
	}
}

// TrackedStreams returns the current number of active streams in the tracker.
func (t *SequenceTracker) TrackedStreams() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.states)
}
