package repository

import (
	"context"

	"rag-telemetry-audit-daemon/internal/domain/entity"
)

// SequenceRange represents a contiguous [MinSeq, MaxSeq] sequence interval for a producer.
type SequenceRange struct {
	ProducerID string
	MinSeq     uint64
	MaxSeq     uint64
}

// HotStorePort defines the persistence contract for the fast forensic hot tier (ClickHouse).
type HotStorePort interface {
	WriteBatch(ctx context.Context, entries []entity.ComplianceEntry) error
	QuerySequenceRanges(ctx context.Context, tenantID string, partition uint16, fromSeq, toSeq uint64) ([]SequenceRange, error)
	Ping(ctx context.Context) error
	Close() error
}
