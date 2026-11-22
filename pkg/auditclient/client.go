package auditclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
)

const (
	// DefaultTopic is the Kafka topic for compliance audit records.
	DefaultTopic = "audit-compliance-records"
	// DefaultNumPartitions is the default partition count for audit-compliance-records.
	DefaultNumPartitions int32 = 8
	// DefaultWriteQueueSize is the bounded queue capacity for write-plane mutations.
	DefaultWriteQueueSize = 1024
	// DefaultReadQueueSize is the bounded queue capacity for read-plane queries.
	DefaultReadQueueSize = 4096
	// DefaultWriteTimeout is the bounded wait timeout for write-plane backpressure.
	DefaultWriteTimeout = 5 * time.Second
	// DefaultDiagnosticBufferSize is the default capacity of the droppable diagnostic ring buffer.
	DefaultDiagnosticBufferSize = 4096
	// DefaultDiagnosticFlushInterval is the default flush cadence for diagnostic spans.
	DefaultDiagnosticFlushInterval = 2 * time.Second
)

var (
	// ErrAuditUnavailable is returned when the write-plane compliance pipeline cannot accept
	// or durably queue a mutation event within the bounded wait timeout. Callers must fail closed (HTTP 503).
	ErrAuditUnavailable = errors.New("audit compliance pipeline unavailable")

	// ErrInvalidRecord is returned when a compliance AuditRecord fails required field validation.
	ErrInvalidRecord = errors.New("invalid audit record")

	// ErrClientClosed is returned when operations are attempted on a closed Client.
	ErrClientClosed = errors.New("audit client closed")
)

// Producer defines the narrow Kafka producer port required by auditclient.
type Producer interface {
	ProduceSync(ctx context.Context, topic string, partition int32, key, value []byte) error
	Close()
}

type franzProducer struct {
	client *kgo.Client
}

func newFranzProducer(brokers []string) (*franzProducer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(), // explicit manual partitioner + acks=all
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &franzProducer{client: cl}, nil
}

func (p *franzProducer) ProduceSync(ctx context.Context, topic string, partition int32, key, value []byte) error {
	rec := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *franzProducer) Close() {
	p.client.Close()
}

// Config configures the dual-channel audit client SDK.
type Config struct {
	Brokers                 []string
	Topic                   string
	NumPartitions           int32
	ProducerID              string
	WriteQueueSize          int
	ReadQueueSize           int
	WriteTimeout            time.Duration
	DiagnosticBufferSize    int
	DiagnosticFlushInterval time.Duration
	DiagnosticSink          DiagnosticSink
	Producer                Producer
	Logger                  *slog.Logger
}

type seqKey struct {
	producerID string
	tenantID   string
	partition  int32
}

type seqTracker struct {
	mu   sync.Mutex
	seqs map[seqKey]uint64
}

func newSeqTracker() *seqTracker {
	return &seqTracker{
		seqs: make(map[seqKey]uint64),
	}
}

func (s *seqTracker) next(producerID, tenantID string, partition int32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := seqKey{
		producerID: producerID,
		tenantID:   tenantID,
		partition:  partition,
	}
	s.seqs[k]++
	return s.seqs[k]
}

// Client is the caller-side dual-channel telemetry and compliance audit SDK.
type Client struct {
	topic         string
	numPartitions int32
	producerID    string
	producer      Producer
	logger        *slog.Logger
	seqs          *seqTracker
	compliance    *complianceChannel
	diagnostic    *RingBuffer

	closeOnce sync.Once
}

// NewClient initializes a dual-channel audit Client with isolated write/read compliance
// queues and a droppable diagnostic ring buffer.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Topic == "" {
		cfg.Topic = DefaultTopic
	}
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = DefaultNumPartitions
	}
	if cfg.WriteQueueSize <= 0 {
		cfg.WriteQueueSize = DefaultWriteQueueSize
	}
	if cfg.ReadQueueSize <= 0 {
		cfg.ReadQueueSize = DefaultReadQueueSize
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWriteTimeout
	}
	if cfg.DiagnosticBufferSize <= 0 {
		cfg.DiagnosticBufferSize = DefaultDiagnosticBufferSize
	}
	if cfg.DiagnosticFlushInterval <= 0 {
		cfg.DiagnosticFlushInterval = DefaultDiagnosticFlushInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	if cfg.ProducerID == "" {
		cfg.ProducerID = defaultProducerID()
	}

	prod := cfg.Producer
	if prod == nil {
		var err error
		prod, err = newFranzProducer(cfg.Brokers)
		if err != nil {
			return nil, err
		}
	}

	seqs := newSeqTracker()
	comp := newComplianceChannel(complianceConfig{
		topic:          cfg.Topic,
		writeQueueSize: cfg.WriteQueueSize,
		readQueueSize:  cfg.ReadQueueSize,
		writeTimeout:   cfg.WriteTimeout,
		producer:       prod,
		seqs:           seqs,
		logger:         cfg.Logger,
	})
	diag := NewRingBuffer(cfg.DiagnosticBufferSize, cfg.DiagnosticFlushInterval, cfg.DiagnosticSink, cfg.Logger)

	return &Client{
		topic:         cfg.Topic,
		numPartitions: cfg.NumPartitions,
		producerID:    cfg.ProducerID,
		producer:      prod,
		logger:        cfg.Logger,
		seqs:          seqs,
		compliance:    comp,
		diagnostic:    diag,
	}, nil
}

// PartitionForTenant deterministically maps a tenant_id to a Kafka partition via FNV-1a 32-bit hash.
func PartitionForTenant(tenantID string, numPartitions int32) int32 {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenantID))
	// #nosec G115 -- numPartitions is positive and bounded by Kafka topic partition count
	return int32(h.Sum32() % uint32(numPartitions))
}

// RecordCompliance validates, enriches, and routes a compliance AuditRecord through
// either the fail-closed write-plane channel or the non-blocking read-plane channel.
func (c *Client) RecordCompliance(ctx context.Context, rec *auditv1.AuditRecord) error {
	if err := validateRecord(rec); err != nil {
		return err
	}

	// Clone top-level struct before enriching so caller-owned pointers are not mutated unexpectedly.
	enriched := proto.Clone(rec).(*auditv1.AuditRecord)
	if enriched.EventId == "" {
		enriched.EventId = newUUIDv4()
	}
	if enriched.EventTime == nil {
		enriched.EventTime = timestamppb.Now()
	}
	if enriched.ProducerId == "" {
		enriched.ProducerId = c.producerID
	}

	partition := PartitionForTenant(enriched.TenantId, c.numPartitions)

	switch enriched.Plane {
	case auditv1.PlaneType_PLANE_TYPE_WRITE:
		return c.compliance.enqueueWrite(ctx, enriched, partition)
	case auditv1.PlaneType_PLANE_TYPE_READ:
		return c.compliance.enqueueRead(ctx, enriched, partition)
	default:
		return fmt.Errorf("%w: unsupported plane type", ErrInvalidRecord)
	}
}

// RecordDiagnostic pushes a non-compliance diagnostic span into the droppable ring buffer.
func (c *Client) RecordDiagnostic(span DiagnosticSpan) {
	c.diagnostic.Push(span)
}

// DroppedRecordsTotal returns the combined count of dropped read-plane compliance records
// and dropped diagnostic spans (audit_caller_dropped_records_total).
func (c *Client) DroppedRecordsTotal() uint64 {
	return c.compliance.DroppedTotal() + c.diagnostic.DroppedTotal()
}

// ShedMetadataTotal returns the count of read-plane records that had non-essential metadata
// shed at >= 90% buffer utilization.
func (c *Client) ShedMetadataTotal() uint64 {
	return c.compliance.ShedTotal()
}

// Close gracefully drains in-flight compliance and diagnostic buffers and closes the Kafka producer.
func (c *Client) Close(ctx context.Context) error {
	var firstErr error
	c.closeOnce.Do(func() {
		if err := c.compliance.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := c.diagnostic.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		c.producer.Close()
	})
	return firstErr
}

func validateRecord(rec *auditv1.AuditRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: record is nil", ErrInvalidRecord)
	}
	if rec.TenantId == "" {
		return fmt.Errorf("%w: tenant_id is required", ErrInvalidRecord)
	}
	if rec.Plane == auditv1.PlaneType_PLANE_TYPE_UNSPECIFIED {
		return fmt.Errorf("%w: plane must be PLANE_TYPE_READ or PLANE_TYPE_WRITE", ErrInvalidRecord)
	}
	if rec.Action == "" {
		return fmt.Errorf("%w: action is required", ErrInvalidRecord)
	}
	if rec.Actor == nil || rec.Actor.ActorId == "" {
		return fmt.Errorf("%w: actor.actor_id is required", ErrInvalidRecord)
	}
	return nil
}

func defaultProducerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "producer"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%08x", host, binary.BigEndian.Uint32(b[:]))
}

func newUUIDv4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
