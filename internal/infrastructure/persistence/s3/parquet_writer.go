package s3

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/parquet-go/parquet-go"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
)

const SchemaVersionV1 = "v1"

// ParquetAuditRecord defines the exact columnar Parquet schema specified in .ai/doc-api.md.
type ParquetAuditRecord struct {
	EventID               string   `parquet:"event_id"`
	TraceID               string   `parquet:"trace_id"`
	TenantID              string   `parquet:"tenant_id"`
	ProducerID            string   `parquet:"producer_id"`
	KafkaPartition        int32    `parquet:"kafka_partition"`
	KafkaOffset           int64    `parquet:"kafka_offset"`
	SeqID                 uint64   `parquet:"seq_id"`
	EventTimeMS           int64    `parquet:"event_time_ms,timestamp(millisecond)"`
	Plane                 int32    `parquet:"plane"`
	Action                string   `parquet:"action"`
	ServiceName           string   `parquet:"service_name"`
	ActorID               string   `parquet:"actor_id"`
	AuthMethod            string   `parquet:"auth_method"`
	SourceIPHash          string   `parquet:"source_ip_hash"`
	DocIDs                []uint32 `parquet:"doc_ids,list"`
	ChunkIDs              []string `parquet:"chunk_ids,list"`
	DocumentVersions      []string `parquet:"document_versions,list"`
	CacheHit              bool     `parquet:"cache_hit"`
	EmbeddingModelVersion string   `parquet:"embedding_model_version"`
	PayloadRedacted       []byte   `parquet:"payload_redacted"`
	PromptTokens          uint32   `parquet:"prompt_tokens"`
	CompletionTokens      uint32   `parquet:"completion_tokens"`
}

// EncodeParquetBatch encodes entries into a Snappy-compressed columnar Parquet buffer
// with [min_seq, max_seq] and producer_id key-value metadata in the Parquet footer.
func EncodeParquetBatch(manifest entity.BatchManifest, entries []entity.ComplianceEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, errors.New("cannot encode empty parquet batch")
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[ParquetAuditRecord](
		&buf,
		parquet.Compression(&parquet.Snappy),
	)

	rows := make([]ParquetAuditRecord, len(entries))
	for i := range entries {
		e := &entries[i]
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("entry %d validation: %w", i, err)
		}
		rows[i] = ParquetAuditRecord{
			EventID:               e.EventID,
			TraceID:               e.TraceID,
			TenantID:              e.TenantID,
			ProducerID:            e.ProducerID,
			KafkaPartition:        int32(e.KafkaPartition),
			KafkaOffset:           int64(e.KafkaOffset), // #nosec G115 -- Kafka offset fits int64
			SeqID:                 e.SeqID,
			EventTimeMS:           e.EventTime.UTC().UnixMilli(),
			Plane:                 int32(e.Plane),
			Action:                e.Action,
			ServiceName:           e.ServiceName,
			ActorID:               e.ActorID,
			AuthMethod:            e.AuthMethod,
			SourceIPHash:          e.SourceIPHash,
			DocIDs:                nonNilUint32s(e.DocIDs),
			ChunkIDs:              nonNilStrings(e.ChunkIDs),
			DocumentVersions:      nonNilStrings(e.DocumentVersions),
			CacheHit:              e.CacheHit,
			EmbeddingModelVersion: e.EmbeddingModelVersion,
			PayloadRedacted:       e.PayloadRedacted,
			PromptTokens:          e.PromptTokens,
			CompletionTokens:      e.CompletionTokens,
		}
	}

	if _, err := writer.Write(rows); err != nil {
		return nil, fmt.Errorf("write parquet rows: %w", err)
	}

	writer.SetKeyValueMetadata("tenant_id", manifest.Key.TenantID)
	writer.SetKeyValueMetadata("producer_id", manifest.Key.ProducerID)
	writer.SetKeyValueMetadata("kafka_partition", strconv.FormatUint(uint64(manifest.Key.KafkaPartition), 10))
	writer.SetKeyValueMetadata("min_seq", strconv.FormatUint(manifest.MinSeq, 10))
	writer.SetKeyValueMetadata("max_seq", strconv.FormatUint(manifest.MaxSeq, 10))
	writer.SetKeyValueMetadata("record_count", strconv.Itoa(len(entries)))
	writer.SetKeyValueMetadata("schema_version", SchemaVersionV1)

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// ReadParquetManifest reads only the Parquet footer from r to extract the
// ColdObjectManifest key-value metadata without downloading row group payloads.
func ReadParquetManifest(r io.ReaderAt, size int64) (repository.ColdObjectManifest, error) {
	if size <= 0 {
		return repository.ColdObjectManifest{}, errors.New("invalid parquet object size")
	}
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return repository.ColdObjectManifest{}, fmt.Errorf("open parquet footer: %w", err)
	}

	meta := f.Metadata()
	kv := make(map[string]string, len(meta.KeyValueMetadata))
	for _, entry := range meta.KeyValueMetadata {
		kv[entry.Key] = entry.Value
	}

	partVal, err := strconv.ParseUint(kv["kafka_partition"], 10, 16)
	if err != nil {
		return repository.ColdObjectManifest{}, fmt.Errorf("parse parquet kafka_partition metadata: %w", err)
	}
	minSeq, err := strconv.ParseUint(kv["min_seq"], 10, 64)
	if err != nil {
		return repository.ColdObjectManifest{}, fmt.Errorf("parse parquet min_seq metadata: %w", err)
	}
	maxSeq, err := strconv.ParseUint(kv["max_seq"], 10, 64)
	if err != nil {
		return repository.ColdObjectManifest{}, fmt.Errorf("parse parquet max_seq metadata: %w", err)
	}
	recCount, err := strconv.ParseInt(kv["record_count"], 10, 64)
	if err != nil {
		recCount = meta.NumRows
	}

	return repository.ColdObjectManifest{
		TenantID:       kv["tenant_id"],
		ProducerID:     kv["producer_id"],
		KafkaPartition: uint16(partVal),
		MinSeq:         minSeq,
		MaxSeq:         maxSeq,
		RecordCount:    recCount,
		SchemaVersion:  kv["schema_version"],
	}, nil
}

func nonNilUint32s(in []uint32) []uint32 {
	if in == nil {
		return []uint32{}
	}
	return in
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}
