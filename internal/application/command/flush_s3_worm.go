package command

import (
	"context"
	"errors"
	"sync"
	"time"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
)

const (
	// DefaultColdFlushInterval is the 60-second S3 WORM Parquet flush interval (P99 <= 60s SLO).
	DefaultColdFlushInterval = 60 * time.Second
	// DefaultColdFlushBytes is the 32 MiB global S3 WORM micro-batch threshold.
	DefaultColdFlushBytes int64 = 32 * 1024 * 1024
	// DefaultColdUploadConcurrency bounds concurrent S3 PutObject calls per flush.
	DefaultColdUploadConcurrency = 4
)

// ColdFlusher micro-batches ComplianceEntry records up to a 60s or 32 MiB global cap
// and flushes per-(tenant_id, kafka_partition, producer_id) Parquet objects to S3 WORM.
type ColdFlusher struct {
	store          repository.ColdStorePort
	maxBytes       int64
	interval       time.Duration
	maxConcurrency int

	mu          sync.Mutex
	buf         []entity.ComplianceEntry
	bufBytes    int64
	flushed     map[uint16]uint64
	lastFlushAt time.Time
}

// NewColdFlusher constructs a ColdFlusher with calibrated defaults when zero values are passed.
func NewColdFlusher(store repository.ColdStorePort, maxBytes int64, interval time.Duration, maxConcurrency int) *ColdFlusher {
	if maxBytes <= 0 {
		maxBytes = DefaultColdFlushBytes
	}
	if interval <= 0 {
		interval = DefaultColdFlushInterval
	}
	if maxConcurrency <= 0 {
		maxConcurrency = DefaultColdUploadConcurrency
	}
	return &ColdFlusher{
		store:          store,
		maxBytes:       maxBytes,
		interval:       interval,
		maxConcurrency: maxConcurrency,
		flushed:        make(map[uint16]uint64),
		lastFlushAt:    time.Now().UTC(),
	}
}

// Add appends entries to the global cold buffer and flushes if the 32 MiB threshold is reached.
// Any failed stream entries are returned so the caller can spill them to mmap_wal.
func (f *ColdFlusher) Add(ctx context.Context, entries []entity.ComplianceEntry) ([]entity.ComplianceEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	var addedBytes int64
	for i := range entries {
		addedBytes += estimateEntryBytes(&entries[i])
	}

	var toFlush []entity.ComplianceEntry
	f.mu.Lock()
	f.buf = append(f.buf, entries...)
	f.bufBytes += addedBytes
	if f.bufBytes >= f.maxBytes {
		toFlush = f.buf
		f.buf = nil
		f.bufBytes = 0
		f.lastFlushAt = time.Now().UTC()
	}
	f.mu.Unlock()

	if len(toFlush) == 0 {
		return nil, nil
	}
	return f.flushEntries(ctx, toFlush)
}

// Flush forces an immediate flush of all buffered cold-tier records.
func (f *ColdFlusher) Flush(ctx context.Context) ([]entity.ComplianceEntry, error) {
	f.mu.Lock()
	if len(f.buf) == 0 {
		f.mu.Unlock()
		return nil, nil
	}
	toFlush := f.buf
	f.buf = nil
	f.bufBytes = 0
	f.lastFlushAt = time.Now().UTC()
	f.mu.Unlock()

	return f.flushEntries(ctx, toFlush)
}

// FlushPartition flushes only buffered records belonging to partition (e.g. on rebalance).
func (f *ColdFlusher) FlushPartition(ctx context.Context, partition uint16) ([]entity.ComplianceEntry, error) {
	f.mu.Lock()
	if len(f.buf) == 0 {
		f.mu.Unlock()
		return nil, nil
	}
	var toFlush []entity.ComplianceEntry
	var remaining []entity.ComplianceEntry
	var remainingBytes int64
	for i := range f.buf {
		if f.buf[i].KafkaPartition == partition {
			toFlush = append(toFlush, f.buf[i])
		} else {
			remaining = append(remaining, f.buf[i])
			remainingBytes += estimateEntryBytes(&f.buf[i])
		}
	}
	f.buf = remaining
	f.bufBytes = remainingBytes
	f.mu.Unlock()

	if len(toFlush) == 0 {
		return nil, nil
	}
	return f.flushEntries(ctx, toFlush)
}

// Interval returns the configured cold flush cadence.
func (f *ColdFlusher) Interval() time.Duration {
	return f.interval
}

// BufferedBytes returns the current estimated byte size of buffered cold-tier records.
func (f *ColdFlusher) BufferedBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bufBytes
}

// FlushedWatermarks returns a copy of the highest S3-persisted KafkaOffset per partition.
func (f *ColdFlusher) FlushedWatermarks() map[uint16]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[uint16]uint64, len(f.flushed))
	for k, v := range f.flushed {
		out[k] = v
	}
	return out
}

func (f *ColdFlusher) flushEntries(ctx context.Context, entries []entity.ComplianceEntry) ([]entity.ComplianceEntry, error) {
	grouped := make(map[entity.StreamKey][]entity.ComplianceEntry)
	for i := range entries {
		k := entity.StreamKey{
			ProducerID:     entries[i].ProducerID,
			TenantID:       entries[i].TenantID,
			KafkaPartition: entries[i].KafkaPartition,
		}
		grouped[k] = append(grouped[k], entries[i])
	}

	type streamJob struct {
		key      entity.StreamKey
		entries  []entity.ComplianceEntry
		manifest entity.BatchManifest
	}
	jobs := make([]streamJob, 0, len(grouped))
	for k, list := range grouped {
		minSeq, maxSeq := list[0].SeqID, list[0].SeqID
		for i := 1; i < len(list); i++ {
			if list[i].SeqID < minSeq {
				minSeq = list[i].SeqID
			}
			if list[i].SeqID > maxSeq {
				maxSeq = list[i].SeqID
			}
		}
		jobs = append(jobs, streamJob{
			key:     k,
			entries: list,
			manifest: entity.BatchManifest{
				Key:         k,
				MinSeq:      minSeq,
				MaxSeq:      maxSeq,
				RecordCount: len(list),
			},
		})
	}

	sem := make(chan struct{}, f.maxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failedEntries []entity.ComplianceEntry
	var errs []error
	failedPartitions := make(map[uint16]bool)
	candidateOffsets := make(map[uint16]uint64)

	for _, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j streamJob) {
			defer wg.Done()
			defer func() { <-sem }()

			err := f.store.WriteBatch(ctx, j.manifest, j.entries)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				failedPartitions[j.key.KafkaPartition] = true
				failedEntries = append(failedEntries, j.entries...)
				return
			}
			for i := range j.entries {
				p := j.entries[i].KafkaPartition
				if j.entries[i].KafkaOffset >= candidateOffsets[p] {
					candidateOffsets[p] = j.entries[i].KafkaOffset
				}
			}
		}(job)
	}
	wg.Wait()

	f.mu.Lock()
	for p, off := range candidateOffsets {
		if !failedPartitions[p] && off >= f.flushed[p] {
			f.flushed[p] = off
		}
	}
	f.mu.Unlock()

	if len(errs) > 0 {
		return failedEntries, errors.Join(errs...)
	}
	return nil, nil
}

func estimateEntryBytes(e *entity.ComplianceEntry) int64 {
	size := int64(256 + len(e.PayloadRedacted) + len(e.Action) + len(e.ActorID) + len(e.EventID) + len(e.TraceID))
	size += int64(len(e.DocIDs) * 4)
	for _, c := range e.ChunkIDs {
		size += int64(len(c))
	}
	for _, v := range e.DocumentVersions {
		size += int64(len(v))
	}
	return size
}
