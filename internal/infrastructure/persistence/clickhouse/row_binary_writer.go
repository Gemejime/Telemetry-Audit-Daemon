package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
)

const insertComplianceRecordsSQL = `INSERT INTO audit_compliance_records (
	event_id, trace_id, tenant_id, producer_id,
	kafka_partition, kafka_offset, seq_id, event_time,
	plane, action, service_name, actor_id, auth_method,
	source_ip_hash, doc_ids, chunk_ids, document_versions,
	cache_hit, embedding_model_version, payload_redacted,
	prompt_tokens, completion_tokens
)`

// BatchConn abstracts driver.Conn for RowBinary batch insertion and unit testing.
type BatchConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Ping(ctx context.Context) error
	Close() error
}

// QueryConn abstracts read queries for cross-tier sequence reconciliation without breaking BatchConn mocks.
type QueryConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// MetricsRecorder receives hot-tier persistence latency observations (audit_hot_persist_duration_seconds).
type MetricsRecorder interface {
	ObserveHotPersistDuration(d time.Duration)
}

// RetryConfig controls exponential backoff behavior for transient ClickHouse write errors.
type RetryConfig struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// RowBinaryWriter implements repository.HotStorePort over ClickHouse native TCP RowBinary.
type RowBinaryWriter struct {
	conn    BatchConn
	retry   RetryConfig
	logger  *slog.Logger
	metrics MetricsRecorder
}

// NewRowBinaryWriter constructs a RowBinaryWriter from a BatchConn (e.g. *Client).
func NewRowBinaryWriter(conn BatchConn, retry RetryConfig, logger *slog.Logger, metrics MetricsRecorder) (*RowBinaryWriter, error) {
	if conn == nil {
		return nil, errors.New("clickhouse connection is required")
	}
	if retry.MaxRetries <= 0 {
		retry.MaxRetries = DefaultMaxRetries
	}
	if retry.BaseBackoff <= 0 {
		retry.BaseBackoff = DefaultRetryBaseBackoff
	}
	if retry.MaxBackoff <= 0 {
		retry.MaxBackoff = DefaultRetryMaxBackoff
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &RowBinaryWriter{
		conn:    conn,
		retry:   retry,
		logger:  logger,
		metrics: metrics,
	}, nil
}

// WriteBatch validates and streams a batch of ComplianceEntry records into ClickHouse
// using native TCP RowBinary with bounded exponential backoff retries.
func (w *RowBinaryWriter) WriteBatch(ctx context.Context, entries []entity.ComplianceEntry) error {
	if len(entries) == 0 {
		return nil
	}
	for i := range entries {
		if err := entries[i].Validate(); err != nil {
			return fmt.Errorf("entry %d validation: %w", i, err)
		}
	}

	start := time.Now()
	minSeq, maxSeq := batchSeqBounds(entries)
	first := entries[0]

	err := w.withRetry(ctx, first.TraceID, first.TenantID, first.KafkaPartition, minSeq, maxSeq, func() error {
		batch, err := w.conn.PrepareBatch(ctx, insertComplianceRecordsSQL)
		if err != nil {
			return fmt.Errorf("prepare clickhouse batch: %w", err)
		}
		for i := range entries {
			e := &entries[i]
			docIDs := e.DocIDs
			if docIDs == nil {
				docIDs = []uint32{}
			}
			chunkIDs := e.ChunkIDs
			if chunkIDs == nil {
				chunkIDs = []string{}
			}
			docVersions := e.DocumentVersions
			if docVersions == nil {
				docVersions = []string{}
			}
			if appendErr := batch.Append(
				e.EventID,
				e.TraceID,
				e.TenantID,
				e.ProducerID,
				e.KafkaPartition,
				e.KafkaOffset,
				e.SeqID,
				e.EventTime.UTC(),
				int8(e.Plane),
				e.Action,
				e.ServiceName,
				e.ActorID,
				e.AuthMethod,
				e.SourceIPHash,
				docIDs,
				chunkIDs,
				docVersions,
				e.CacheHit,
				e.EmbeddingModelVersion,
				string(e.PayloadRedacted),
				e.PromptTokens,
				e.CompletionTokens,
			); appendErr != nil {
				_ = batch.Abort()
				return fmt.Errorf("append row %d: %w", i, appendErr)
			}
		}
		if sendErr := batch.Send(); sendErr != nil {
			return fmt.Errorf("send clickhouse batch: %w", sendErr)
		}
		return nil
	})

	elapsed := time.Since(start)
	if w.metrics != nil {
		w.metrics.ObserveHotPersistDuration(elapsed)
	}
	if err != nil {
		return err
	}

	w.logger.Info("clickhouse batch persisted",
		slog.String("trace_id", first.TraceID),
		slog.String("tenant_id", first.TenantID),
		slog.Uint64("partition", uint64(first.KafkaPartition)),
		slog.Uint64("min_seq", minSeq),
		slog.Uint64("max_seq", maxSeq),
		slog.Int("record_count", len(entries)),
		slog.Int64("duration_ms", elapsed.Milliseconds()),
	)
	return nil
}

// Ping verifies ClickHouse connectivity.
func (w *RowBinaryWriter) Ping(ctx context.Context) error {
	return w.conn.Ping(ctx)
}

const selectSequenceRangesSQL = `SELECT producer_id, seq_id
FROM audit_compliance_records FINAL
WHERE tenant_id = ? AND kafka_partition = ? AND seq_id >= ? AND seq_id <= ?
ORDER BY producer_id ASC, seq_id ASC`

// QuerySequenceRanges queries FINAL-deduplicated ClickHouse rows for [fromSeq, toSeq]
// and coalesces contiguous seq_id runs per producer_id.
func (w *RowBinaryWriter) QuerySequenceRanges(
	ctx context.Context,
	tenantID string,
	partition uint16,
	fromSeq, toSeq uint64,
) ([]repository.SequenceRange, error) {
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	if fromSeq > toSeq {
		return nil, errors.New("from_seq must be <= to_seq")
	}
	qc, ok := w.conn.(QueryConn)
	if !ok {
		return nil, errors.New("clickhouse connection does not support queries")
	}

	rows, err := qc.Query(ctx, selectSequenceRangesSQL, tenantID, partition, fromSeq, toSeq)
	if err != nil {
		return nil, fmt.Errorf("query clickhouse sequence ranges: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var ranges []repository.SequenceRange
	for rows.Next() {
		var producerID string
		var seqID uint64
		if err := rows.Scan(&producerID, &seqID); err != nil {
			return nil, fmt.Errorf("scan clickhouse sequence row: %w", err)
		}
		n := len(ranges)
		if n > 0 && ranges[n-1].ProducerID == producerID {
			if seqID <= ranges[n-1].MaxSeq {
				continue
			}
			if seqID == ranges[n-1].MaxSeq+1 {
				ranges[n-1].MaxSeq = seqID
				continue
			}
		}
		ranges = append(ranges, repository.SequenceRange{
			ProducerID: producerID,
			MinSeq:     seqID,
			MaxSeq:     seqID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clickhouse sequence rows: %w", err)
	}
	return ranges, nil
}

// Close closes the underlying ClickHouse connection.
func (w *RowBinaryWriter) Close() error {
	return w.conn.Close()
}

func (w *RowBinaryWriter) withRetry(
	ctx context.Context,
	traceID, tenantID string,
	partition uint16,
	minSeq, maxSeq uint64,
	fn func() error,
) error {
	backoff := w.retry.BaseBackoff
	var lastErr error

	for attempt := 0; attempt <= w.retry.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == w.retry.MaxRetries {
			break
		}

		w.logger.Warn("clickhouse write attempt failed, retrying",
			slog.String("trace_id", traceID),
			slog.String("tenant_id", tenantID),
			slog.Uint64("partition", uint64(partition)),
			slog.Uint64("min_seq", minSeq),
			slog.Uint64("max_seq", maxSeq),
			slog.Int("attempt", attempt+1),
			slog.Int64("backoff_ms", backoff.Milliseconds()),
			slog.String("error", lastErr.Error()),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > w.retry.MaxBackoff {
			backoff = w.retry.MaxBackoff
		}
	}

	return lastErr
}

func batchSeqBounds(entries []entity.ComplianceEntry) (minSeq, maxSeq uint64) {
	minSeq = entries[0].SeqID
	maxSeq = entries[0].SeqID
	for i := 1; i < len(entries); i++ {
		if entries[i].SeqID < minSeq {
			minSeq = entries[i].SeqID
		}
		if entries[i].SeqID > maxSeq {
			maxSeq = entries[i].SeqID
		}
	}
	return minSeq, maxSeq
}
