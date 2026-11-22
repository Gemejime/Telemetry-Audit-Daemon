package command

import (
	"context"
	"sync"
	"time"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
)

const (
	// DefaultHotFlushInterval is the 1,000 ms ClickHouse flush interval (P99 <= 2s SLO).
	DefaultHotFlushInterval = 1000 * time.Millisecond
	// DefaultHotFlushRecords is the 2,500-record ClickHouse batch threshold.
	DefaultHotFlushRecords = 2500
)

// HotFlusher micro-batches ComplianceEntry records and flushes them to ClickHouse
// every 1,000 ms or 2,500 records.
type HotFlusher struct {
	store      repository.HotStorePort
	maxRecords int
	interval   time.Duration

	mu      sync.Mutex
	buf     []entity.ComplianceEntry
	flushed map[uint16]uint64
}

// NewHotFlusher constructs a HotFlusher with calibrated defaults when zero values are passed.
func NewHotFlusher(store repository.HotStorePort, maxRecords int, interval time.Duration) *HotFlusher {
	if maxRecords <= 0 {
		maxRecords = DefaultHotFlushRecords
	}
	if interval <= 0 {
		interval = DefaultHotFlushInterval
	}
	return &HotFlusher{
		store:      store,
		maxRecords: maxRecords,
		interval:   interval,
		buf:        make([]entity.ComplianceEntry, 0, maxRecords),
		flushed:    make(map[uint16]uint64),
	}
}

// Add appends entries to the hot buffer and flushes immediately if maxRecords is reached.
// If flushing fails, the failed batch is returned so the caller can spill it to mmap_wal.
func (f *HotFlusher) Add(ctx context.Context, entries []entity.ComplianceEntry) ([]entity.ComplianceEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	var toFlush []entity.ComplianceEntry
	f.mu.Lock()
	f.buf = append(f.buf, entries...)
	if len(f.buf) >= f.maxRecords {
		toFlush = f.buf
		f.buf = make([]entity.ComplianceEntry, 0, f.maxRecords)
	}
	f.mu.Unlock()

	if len(toFlush) == 0 {
		return nil, nil
	}
	if err := f.flushBatch(ctx, toFlush); err != nil {
		return toFlush, err
	}
	return nil, nil
}

// Flush forces an immediate flush of all buffered hot-tier records.
func (f *HotFlusher) Flush(ctx context.Context) ([]entity.ComplianceEntry, error) {
	f.mu.Lock()
	if len(f.buf) == 0 {
		f.mu.Unlock()
		return nil, nil
	}
	toFlush := f.buf
	f.buf = make([]entity.ComplianceEntry, 0, f.maxRecords)
	f.mu.Unlock()

	if err := f.flushBatch(ctx, toFlush); err != nil {
		return toFlush, err
	}
	return nil, nil
}

// FlushPartition flushes only buffered records belonging to partition (e.g. on rebalance).
func (f *HotFlusher) FlushPartition(ctx context.Context, partition uint16) ([]entity.ComplianceEntry, error) {
	f.mu.Lock()
	if len(f.buf) == 0 {
		f.mu.Unlock()
		return nil, nil
	}
	var toFlush []entity.ComplianceEntry
	remaining := make([]entity.ComplianceEntry, 0, len(f.buf))
	for i := range f.buf {
		if f.buf[i].KafkaPartition == partition {
			toFlush = append(toFlush, f.buf[i])
		} else {
			remaining = append(remaining, f.buf[i])
		}
	}
	f.buf = remaining
	f.mu.Unlock()

	if len(toFlush) == 0 {
		return nil, nil
	}
	if err := f.flushBatch(ctx, toFlush); err != nil {
		return toFlush, err
	}
	return nil, nil
}

// Interval returns the configured flush cadence.
func (f *HotFlusher) Interval() time.Duration {
	return f.interval
}

// FlushedWatermarks returns a copy of the highest persisted KafkaOffset per partition.
func (f *HotFlusher) FlushedWatermarks() map[uint16]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[uint16]uint64, len(f.flushed))
	for k, v := range f.flushed {
		out[k] = v
	}
	return out
}

func (f *HotFlusher) flushBatch(ctx context.Context, batch []entity.ComplianceEntry) error {
	if err := f.store.WriteBatch(ctx, batch); err != nil {
		return err
	}
	f.mu.Lock()
	for i := range batch {
		p := batch[i].KafkaPartition
		if batch[i].KafkaOffset >= f.flushed[p] {
			f.flushed[p] = batch[i].KafkaOffset
		}
	}
	f.mu.Unlock()
	return nil
}
