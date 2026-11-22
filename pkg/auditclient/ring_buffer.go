package auditclient

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// DiagnosticSpan represents a non-compliance performance trace or waterfall span
// routed asynchronously to an OpenTelemetry Collector (droppable under load).
type DiagnosticSpan struct {
	TraceID    string
	TenantID   string
	Name       string
	StartTime  time.Time
	Duration   time.Duration
	Attributes map[string]string
}

// DiagnosticSink exports batched diagnostic spans to an external collector (e.g., OTel gRPC).
type DiagnosticSink interface {
	Export(ctx context.Context, spans []DiagnosticSpan) error
}

// RingBuffer is a bounded, non-blocking, droppable circular buffer for diagnostic spans.
// It tracks dropped spans via DroppedTotal (audit_caller_dropped_records_total).
type RingBuffer struct {
	mu       sync.Mutex
	buf      []DiagnosticSpan
	head     int
	tail     int
	count    int
	capacity int
	closed   bool

	dropped atomic.Uint64
	sink    DiagnosticSink
	logger  *slog.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewRingBuffer creates a bounded droppable diagnostic ring buffer with background flushing.
func NewRingBuffer(capacity int, flushInterval time.Duration, sink DiagnosticSink, logger *slog.Logger) *RingBuffer {
	if capacity <= 0 {
		capacity = DefaultDiagnosticBufferSize
	}
	if flushInterval <= 0 {
		flushInterval = DefaultDiagnosticFlushInterval
	}
	if logger == nil {
		logger = slog.Default()
	}

	rb := &RingBuffer{
		buf:      make([]DiagnosticSpan, capacity),
		capacity: capacity,
		sink:     sink,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}

	rb.wg.Add(1)
	go rb.runFlusher(flushInterval)
	return rb
}

// Push inserts a diagnostic span without blocking. If the buffer is full, the oldest
// span is overwritten and the dropped counter is incremented.
func (r *RingBuffer) Push(span DiagnosticSpan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		r.dropped.Add(1)
		return
	}

	if r.count == r.capacity {
		// Overwrite oldest span to preserve latest diagnostic telemetry without blocking.
		r.head = (r.head + 1) % r.capacity
		r.count--
		r.dropped.Add(1)
	}

	r.buf[r.tail] = span
	r.tail = (r.tail + 1) % r.capacity
	r.count++
}

// Drain removes and returns up to maxItems spans from the ring buffer in FIFO order.
func (r *RingBuffer) Drain(maxItems int) []DiagnosticSpan {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return nil
	}
	n := r.count
	if maxItems > 0 && maxItems < n {
		n = maxItems
	}

	out := make([]DiagnosticSpan, n)
	for i := 0; i < n; i++ {
		out[i] = r.buf[r.head]
		r.buf[r.head] = DiagnosticSpan{}
		r.head = (r.head + 1) % r.capacity
	}
	r.count -= n
	return out
}

// Len returns the current number of buffered diagnostic spans.
func (r *RingBuffer) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// DroppedTotal returns the total number of dropped diagnostic spans (audit_caller_dropped_records_total).
func (r *RingBuffer) DroppedTotal() uint64 {
	return r.dropped.Load()
}

// Close stops the background flusher and performs a final flush of remaining spans.
func (r *RingBuffer) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.stopCh)
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return r.flush(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *RingBuffer) runFlusher(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			if err := r.flush(ctx); err != nil {
				r.logger.Warn("diagnostic span flush failed",
					slog.String("error", err.Error()),
				)
			}
			cancel()
		case <-r.stopCh:
			return
		}
	}
}

func (r *RingBuffer) flush(ctx context.Context) error {
	if r.sink == nil {
		// Drain to prevent unbounded retention when no diagnostic sink is attached.
		_ = r.Drain(0)
		return nil
	}
	spans := r.Drain(0)
	if len(spans) == 0 {
		return nil
	}
	return r.sink.Export(ctx, spans)
}
