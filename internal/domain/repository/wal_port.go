package repository

import (
	"context"
	"errors"
)

var (
	// ErrWALFull is returned when the local spillover WAL reaches its 5 GiB hard cap,
	// signaling backpressure to halt Kafka offset advancement without dropping frames.
	ErrWALFull = errors.New("wal capacity limit reached")

	// ErrWALCorrupt is returned when a WAL record fails CRC32 integrity verification.
	ErrWALCorrupt = errors.New("wal record crc32 mismatch")
)

// WALRecord represents a framed compliance payload read from the local spillover WAL.
type WALRecord struct {
	SegmentID uint64
	Offset    int64
	Payload   []byte
}

// WALPort defines the bounded local NVMe spillover contract for downstream outages.
type WALPort interface {
	Append(ctx context.Context, payload []byte) error
	Drain(ctx context.Context, limit int) ([]WALRecord, error)
	Ack(ctx context.Context, segmentID uint64, offset int64) error
	ActiveBytes() int64
	IsSpilloverActive() bool
	Close() error
}
