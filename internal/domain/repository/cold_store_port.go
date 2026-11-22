package repository

import (
	"context"

	"rag-telemetry-audit-daemon/internal/domain/entity"
)

// ColdStorePort defines the persistence contract for the immutable WORM cold vault (AWS S3).
type ColdStorePort interface {
	WriteBatch(ctx context.Context, manifest entity.BatchManifest, entries []entity.ComplianceEntry) error
	Close() error
}
