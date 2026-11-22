package auditclient

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
)

type complianceConfig struct {
	topic          string
	writeQueueSize int
	readQueueSize  int
	writeTimeout   time.Duration
	producer       Producer
	seqs           *seqTracker
	logger         *slog.Logger
}

type writeRequest struct {
	ctx       context.Context
	rec       *auditv1.AuditRecord
	partition int32
	resultCh  chan error
}

type readItem struct {
	rec       *auditv1.AuditRecord
	partition int32
}

type complianceChannel struct {
	topic        string
	writeTimeout time.Duration
	producer     Producer
	seqs         *seqTracker
	logger       *slog.Logger

	writeQueue chan writeRequest
	readQueue  chan readItem

	droppedRead atomic.Uint64
	shedRead    atomic.Uint64

	mu          sync.Mutex
	closed      bool
	closeOnce   sync.Once
	doneCh      chan struct{}
	workersDone chan struct{}
	enqueueWG   sync.WaitGroup
	wg          sync.WaitGroup
}

func newComplianceChannel(cfg complianceConfig) *complianceChannel {
	c := &complianceChannel{
		topic:        cfg.topic,
		writeTimeout: cfg.writeTimeout,
		producer:     cfg.producer,
		seqs:         cfg.seqs,
		logger:       cfg.logger,
		writeQueue:   make(chan writeRequest, cfg.writeQueueSize),
		readQueue:    make(chan readItem, cfg.readQueueSize),
		doneCh:       make(chan struct{}),
		workersDone:  make(chan struct{}),
	}

	c.wg.Add(2)
	go c.runWriteWorker()
	go c.runReadWorker()
	return c
}

func (c *complianceChannel) enqueueWrite(ctx context.Context, rec *auditv1.AuditRecord, partition int32) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.enqueueWG.Add(1)
	c.mu.Unlock()

	reqCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()

	resCh := make(chan error, 1)
	req := writeRequest{
		ctx:       reqCtx,
		rec:       rec,
		partition: partition,
		resultCh:  resCh,
	}

	select {
	case c.writeQueue <- req:
		c.enqueueWG.Done()
	case <-reqCtx.Done():
		c.enqueueWG.Done()
		if ctx.Err() != nil {
			return fmt.Errorf("%w: context canceled", ErrAuditUnavailable)
		}
		c.logger.Error("write-plane compliance queue full",
			slog.String("trace_id", rec.TraceId),
			slog.String("tenant_id", rec.TenantId),
			slog.Int("partition", int(partition)),
		)
		return fmt.Errorf("%w: write queue timeout", ErrAuditUnavailable)
	case <-c.doneCh:
		c.enqueueWG.Done()
		return ErrClientClosed
	}

	select {
	case err := <-resCh:
		return err
	case <-reqCtx.Done():
		if ctx.Err() != nil {
			return fmt.Errorf("%w: context canceled", ErrAuditUnavailable)
		}
		return fmt.Errorf("%w: write ack timeout", ErrAuditUnavailable)
	}
}

func (c *complianceChannel) enqueueRead(_ context.Context, rec *auditv1.AuditRecord, partition int32) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.enqueueWG.Add(1)
	c.mu.Unlock()
	defer c.enqueueWG.Done()

	qCap := cap(c.readQueue)
	qLen := len(c.readQueue)

	toSend := rec
	if qCap > 0 && qLen*10 >= qCap*9 {
		toSend = shedReadMetadata(rec)
		c.shedRead.Add(1)
		c.logger.Warn("read-plane compliance buffer >= 90% full, shedding non-essential metadata",
			slog.String("trace_id", rec.TraceId),
			slog.String("tenant_id", rec.TenantId),
			slog.Int("partition", int(partition)),
		)
	}

	select {
	case c.readQueue <- readItem{rec: toSend, partition: partition}:
		return nil
	default:
		c.droppedRead.Add(1)
		c.logger.Warn("read-plane compliance buffer full, dropping record",
			slog.String("trace_id", rec.TraceId),
			slog.String("tenant_id", rec.TenantId),
			slog.Int("partition", int(partition)),
		)
		return nil
	}
}

func (c *complianceChannel) runWriteWorker() {
	defer c.wg.Done()
	for req := range c.writeQueue {
		if err := req.ctx.Err(); err != nil {
			req.resultCh <- fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
			continue
		}
		err := c.publish(req.ctx, req.rec, req.partition)
		if err != nil {
			req.resultCh <- fmt.Errorf("%w: publish failed", ErrAuditUnavailable)
		} else {
			req.resultCh <- nil
		}
	}
}

func (c *complianceChannel) runReadWorker() {
	defer c.wg.Done()
	for item := range c.readQueue {
		ctx, cancel := context.WithTimeout(context.Background(), c.writeTimeout)
		if err := c.publish(ctx, item.rec, item.partition); err != nil {
			c.droppedRead.Add(1)
			c.logger.Error("read-plane compliance publish failed",
				slog.String("trace_id", item.rec.TraceId),
				slog.String("tenant_id", item.rec.TenantId),
				slog.Int("partition", int(item.partition)),
				slog.String("error", "kafka publish error"),
			)
		}
		cancel()
	}
}

func (c *complianceChannel) publish(ctx context.Context, rec *auditv1.AuditRecord, partition int32) error {
	// Assign monotonic seq_id only upon acceptance into the publish pipeline.
	rec.SeqId = c.seqs.next(rec.ProducerId, rec.TenantId, partition)

	payload, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal audit record: %w", err)
	}

	start := time.Now()
	if err := c.producer.ProduceSync(ctx, c.topic, partition, []byte(rec.TenantId), payload); err != nil {
		return err
	}

	c.logger.Debug("compliance audit record published",
		slog.String("trace_id", rec.TraceId),
		slog.String("tenant_id", rec.TenantId),
		slog.Int("partition", int(partition)),
		slog.Uint64("seq_id", rec.SeqId),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	)
	return nil
}

func (c *complianceChannel) DroppedTotal() uint64 {
	return c.droppedRead.Load()
}

func (c *complianceChannel) ShedTotal() uint64 {
	return c.shedRead.Load()
}

func (c *complianceChannel) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.doneCh)
		c.mu.Unlock()

		go func() {
			c.enqueueWG.Wait()
			close(c.writeQueue)
			close(c.readQueue)
			c.wg.Wait()
			close(c.workersDone)
		}()
	})

	select {
	case <-c.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shedReadMetadata strips non-essential fields while preserving the principal access event.
func shedReadMetadata(rec *auditv1.AuditRecord) *auditv1.AuditRecord {
	cloned := proto.Clone(rec).(*auditv1.AuditRecord)
	cloned.PayloadRedacted = nil
	cloned.PromptTokens = 0
	cloned.CompletionTokens = 0
	if cloned.AccessTrace != nil {
		cloned.AccessTrace.ChunkIds = nil
		cloned.AccessTrace.DocumentVersions = nil
	}
	return cloned
}
