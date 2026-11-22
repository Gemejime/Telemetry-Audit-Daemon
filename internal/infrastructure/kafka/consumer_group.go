package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"rag-telemetry-audit-daemon/internal/application/command"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

const (
	DefaultTopic            = "audit-compliance-records"
	DefaultGroupID          = "rag-telemetry-audit-daemon"
	DefaultWALDrainInterval = 2 * time.Second
	DefaultWALDrainBatch    = 512
)

// ConsumerConfig configures the stateless Kafka compliance consumer group.
type ConsumerConfig struct {
	Brokers          []string
	Topic            string
	GroupID          string
	WALDrainInterval time.Duration
	WALDrainBatch    int
	Processor        *command.ProcessKafkaBatch
	HotFlusher       *command.HotFlusher
	ColdFlusher      *command.ColdFlusher
	WAL              repository.WALPort
	Logger           *slog.Logger
	Metrics          *observability.Metrics
}

// ConsumerGroup consumes audit-compliance-records, coordinates hot/cold flushes,
// halts offset commits during WAL spillover, and commits offsets only after both tiers persist.
type ConsumerGroup struct {
	client           *kgo.Client
	topic            string
	walDrainInterval time.Duration
	walDrainBatch    int
	processor        *command.ProcessKafkaBatch
	hot              *command.HotFlusher
	cold             *command.ColdFlusher
	wal              repository.WALPort
	logger           *slog.Logger
	metrics          *observability.Metrics

	mu              sync.Mutex
	assigned        map[int32]bool
	committedOffset map[int32]int64
	paused          bool

	healthy  atomic.Bool
	draining atomic.Bool
}

// NewConsumerGroup creates a franz-go consumer group with manual offset commits.
func NewConsumerGroup(cfg ConsumerConfig) (*ConsumerGroup, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if cfg.Processor == nil {
		return nil, errors.New("process kafka batch handler is required")
	}
	if cfg.Topic == "" {
		cfg.Topic = DefaultTopic
	}
	if cfg.GroupID == "" {
		cfg.GroupID = DefaultGroupID
	}
	if cfg.WALDrainInterval <= 0 {
		cfg.WALDrainInterval = DefaultWALDrainInterval
	}
	if cfg.WALDrainBatch <= 0 {
		cfg.WALDrainBatch = DefaultWALDrainBatch
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	cg := &ConsumerGroup{
		topic:            cfg.Topic,
		walDrainInterval: cfg.WALDrainInterval,
		walDrainBatch:    cfg.WALDrainBatch,
		processor:        cfg.Processor,
		hot:              cfg.HotFlusher,
		cold:             cfg.ColdFlusher,
		wal:              cfg.WAL,
		logger:           cfg.Logger,
		metrics:          cfg.Metrics,
		assigned:         make(map[int32]bool),
		committedOffset:  make(map[int32]int64),
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(cg.onAssigned),
		kgo.OnPartitionsRevoked(cg.onRevoked),
		kgo.OnPartitionsLost(cg.onLost),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer group: %w", err)
	}

	cg.client = client
	cg.healthy.Store(true)
	return cg, nil
}

// Run starts the Kafka poll loop, periodic hot/cold flushers, and WAL recovery loop
// until ctx is cancelled, then drains and commits safe offsets.
func (cg *ConsumerGroup) Run(ctx context.Context) error {
	hotInterval := command.DefaultHotFlushInterval
	if cg.hot != nil {
		hotInterval = cg.hot.Interval()
	}
	coldInterval := command.DefaultColdFlushInterval
	if cg.cold != nil {
		coldInterval = cg.cold.Interval()
	}

	hotTicker := time.NewTicker(hotInterval)
	coldTicker := time.NewTicker(coldInterval)
	walTicker := time.NewTicker(cg.walDrainInterval)
	defer hotTicker.Stop()
	defer coldTicker.Stop()
	defer walTicker.Stop()

	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-bgCtx.Done():
				return
			case <-hotTicker.C:
				if cg.hot != nil {
					if _, err := cg.hot.Flush(bgCtx); err != nil {
						cg.logger.Warn("periodic hot flush failed", slog.String("error", err.Error()))
					}
					cg.commitSafeOffsets(bgCtx)
				}
			case <-coldTicker.C:
				if cg.cold != nil {
					if _, err := cg.cold.Flush(bgCtx); err != nil {
						cg.logger.Warn("periodic cold flush failed", slog.String("error", err.Error()))
					}
					cg.commitSafeOffsets(bgCtx)
				}
			case <-walTicker.C:
				if _, err := cg.processor.DrainWALOnce(bgCtx, cg.walDrainBatch); err != nil {
					cg.logger.Warn("wal drain attempt failed", slog.String("error", err.Error()))
				} else if cg.wal == nil || !cg.wal.IsSpilloverActive() {
					cg.resumeIfPaused()
					cg.commitSafeOffsets(bgCtx)
				}
			}
		}
	}()

	for {
		fetches := cg.client.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) {
					continue
				}
				cg.healthy.Store(false)
				cg.logger.Error("kafka fetch error",
					slog.Uint64("partition", uint64(fe.Partition)), // #nosec G115 -- Kafka partition >= 0
					slog.String("error", fe.Err.Error()),
				)
			}
		} else {
			cg.healthy.Store(true)
		}

		records := make([]command.RawKafkaRecord, 0, fetches.NumRecords())
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, command.RawKafkaRecord{
				Partition: uint16(r.Partition), // #nosec G115 -- Kafka partition < 65536
				Offset:    uint64(r.Offset),    // #nosec G115 -- Kafka offset >= 0
				Value:     r.Value,
			})
		})

		if len(records) > 0 {
			if err := cg.processor.Handle(ctx, records); err != nil {
				if errors.Is(err, repository.ErrWALFull) {
					cg.pauseAssignedPartitions()
				}
				cg.logger.Warn("kafka batch processing encountered storage error",
					slog.Int("record_count", len(records)),
					slog.String("error", err.Error()),
				)
			}
			cg.commitSafeOffsets(ctx)
		}
	}

	bgCancel()
	wg.Wait()

	// Graceful drain on shutdown.
	cg.draining.Store(true)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer drainCancel()

	if err := cg.processor.FlushAll(drainCtx); err != nil {
		cg.logger.Warn("shutdown flush encountered error", slog.String("error", err.Error()))
	}
	cg.commitSafeOffsets(drainCtx)
	cg.client.Close()
	return nil
}

// IsHealthy reports whether the Kafka consumer group is healthy.
func (cg *ConsumerGroup) IsHealthy() bool {
	return cg.healthy.Load()
}

// IsDraining reports whether the consumer group is in SIGTERM drain mode.
func (cg *ConsumerGroup) IsDraining() bool {
	return cg.draining.Load()
}

// SetDraining marks the consumer group as draining for readiness probes.
func (cg *ConsumerGroup) SetDraining(draining bool) {
	cg.draining.Store(draining)
}

func (cg *ConsumerGroup) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	cg.mu.Lock()
	for _, parts := range assigned {
		for _, p := range parts {
			cg.assigned[p] = true
		}
	}
	cg.mu.Unlock()
}

func (cg *ConsumerGroup) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for _, parts := range revoked {
		for _, p := range parts {
			if err := cg.processor.FlushPartition(ctx, uint16(p)); err != nil { // #nosec G115
				cg.logger.Warn("rebalance flush failed",
					slog.Uint64("partition", uint64(p)), // #nosec G115
					slog.String("error", err.Error()),
				)
			}
		}
	}
	cg.commitSafeOffsets(ctx)

	cg.mu.Lock()
	for _, parts := range revoked {
		for _, p := range parts {
			delete(cg.assigned, p)
			delete(cg.committedOffset, p)
		}
	}
	cg.mu.Unlock()
}

func (cg *ConsumerGroup) onLost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	cg.onRevoked(ctx, cl, lost)
}

func (cg *ConsumerGroup) pauseAssignedPartitions() {
	cg.mu.Lock()
	if cg.paused || len(cg.assigned) == 0 {
		cg.mu.Unlock()
		return
	}
	parts := make([]int32, 0, len(cg.assigned))
	for p := range cg.assigned {
		parts = append(parts, p)
	}
	cg.paused = true
	cg.mu.Unlock()

	cg.client.PauseFetchPartitions(map[string][]int32{cg.topic: parts})
	cg.logger.Warn("paused kafka fetch partitions due to wal capacity limit",
		slog.Int("partition_count", len(parts)),
	)
}

func (cg *ConsumerGroup) resumeIfPaused() {
	cg.mu.Lock()
	if !cg.paused {
		cg.mu.Unlock()
		return
	}
	parts := make([]int32, 0, len(cg.assigned))
	for p := range cg.assigned {
		parts = append(parts, p)
	}
	cg.paused = false
	cg.mu.Unlock()

	if len(parts) > 0 {
		cg.client.ResumeFetchPartitions(map[string][]int32{cg.topic: parts})
		cg.logger.Info("resumed kafka fetch partitions after wal drain",
			slog.Int("partition_count", len(parts)),
		)
	}
}

func (cg *ConsumerGroup) commitSafeOffsets(ctx context.Context) {
	if cg.wal != nil && cg.wal.IsSpilloverActive() {
		return
	}

	var hotMarks, coldMarks map[uint16]uint64
	if cg.hot != nil {
		hotMarks = cg.hot.FlushedWatermarks()
	}
	if cg.cold != nil {
		coldMarks = cg.cold.FlushedWatermarks()
	}
	if len(hotMarks) == 0 || len(coldMarks) == 0 {
		return
	}

	offsets := make(map[string]map[int32]kgo.EpochOffset)
	topicOffsets := make(map[int32]kgo.EpochOffset)

	cg.mu.Lock()
	for part, hotOff := range hotMarks {
		coldOff, ok := coldMarks[part]
		if !ok {
			continue
		}
		safeOff := hotOff
		if coldOff < safeOff {
			safeOff = coldOff
		}
		nextCommit := int64(safeOff + 1) // #nosec G115 -- Kafka offset fits int64
		p32 := int32(part)
		if prev, exists := cg.committedOffset[p32]; exists && nextCommit <= prev {
			continue
		}
		topicOffsets[p32] = kgo.EpochOffset{Epoch: -1, Offset: nextCommit}
	}
	cg.mu.Unlock()

	if len(topicOffsets) == 0 {
		return
	}
	offsets[cg.topic] = topicOffsets

	var commitErr error
	cg.client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
	})
	if commitErr != nil {
		cg.logger.Warn("failed to commit kafka offsets", slog.String("error", commitErr.Error()))
		return
	}

	cg.mu.Lock()
	for p32, eo := range topicOffsets {
		cg.committedOffset[p32] = eo.Offset
	}
	cg.mu.Unlock()
}
