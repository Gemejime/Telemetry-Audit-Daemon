package repository

import (
	"context"

	"rag-telemetry-audit-daemon/internal/domain/entity"
)

// ColdObjectManifest describes the sequence interval and metadata stored in an S3 Parquet footer.
type ColdObjectManifest struct {
	Key            string
	TenantID       string
	ProducerID     string
	KafkaPartition uint16
	MinSeq         uint64
	MaxSeq         uint64
	RecordCount    int64
	SchemaVersion  string
}

// ColdStorePort defines the persistence contract for the immutable WORM cold vault (AWS S3).
type ColdStorePort interface {
	WriteBatch(ctx context.Context, manifest entity.BatchManifest, entries []entity.ComplianceEntry) error
	ListManifests(ctx context.Context, tenantID string, partition uint16, fromSeq, toSeq uint64) ([]ColdObjectManifest, error)
	Close() error
}
