package repository

import (
	"context"

	"rag-telemetry-audit-daemon/internal/domain/entity"
)

// HotStorePort defines the persistence contract for the fast forensic hot tier (ClickHouse).
type HotStorePort interface {
	WriteBatch(ctx context.Context, entries []entity.ComplianceEntry) error
	Ping(ctx context.Context) error
	Close() error
}
