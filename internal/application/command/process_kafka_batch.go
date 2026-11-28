package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

// RawKafkaRecord represents an unparsed Kafka message from audit-compliance-records.
type RawKafkaRecord struct {
	Partition uint16
	Offset    uint64
	Value     []byte
}

// walEnvelope preserves redacted ComplianceEntry fields and target tier flags in mmap_wal.
type walEnvelope struct {
	NeedHot  bool                   `json:"need_hot"`
	NeedCold bool                   `json:"need_cold"`
	Entry    entity.ComplianceEntry `json:"entry"`
}

// ProcessKafkaBatch decodes, validates, redacts in-flight, tracks sequence continuity,
// dispatches to ClickHouse and S3 flushers, and spills to mmap_wal on downstream failures.
type ProcessKafkaBatch struct {
	redactor        *redaction.Engine
	seqTrack        *entity.SequenceTracker
	hot             *HotFlusher
	cold            *ColdFlusher
	hotStore        repository.HotStorePort
	coldStore       repository.ColdStorePort
	wal             repository.WALPort
	logger          *slog.Logger
	metrics         *observability.Metrics
	producerTenants map[string]string
}

// SetProducerTenantBindings configures a trusted producer_id -> tenant_id registry
// enforced during batch ingestion before records reach either flusher.
func (p *ProcessKafkaBatch) SetProducerTenantBindings(bindings map[string]string) {
	if len(bindings) == 0 {
		p.producerTenants = nil
		return
	}
	cloned := make(map[string]string, len(bindings))
	for k, v := range bindings {
		cloned[k] = v
	}
	p.producerTenants = cloned
}

// NewProcessKafkaBatch constructs the compliance batch processor command handler.
func NewProcessKafkaBatch(
	redactor *redaction.Engine,
	seqTrack *entity.SequenceTracker,
	hot *HotFlusher,
	cold *ColdFlusher,
	hotStore repository.HotStorePort,
	coldStore repository.ColdStorePort,
	wal repository.WALPort,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *ProcessKafkaBatch {
	if redactor == nil {
		redactor = redaction.NewEngine(nil)
	}
	if seqTrack == nil {
		seqTrack = entity.NewSequenceTracker(entity.DefaultMaxTrackedStreams)
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &ProcessKafkaBatch{
		redactor:  redactor,
		seqTrack:  seqTrack,
		hot:       hot,
		cold:      cold,
		hotStore:  hotStore,
		coldStore: coldStore,
		wal:       wal,
		logger:    logger,
		metrics:   metrics,
	}
}

// Handle decodes, validates, redacts, tracks sequences, and dispatches a Kafka record batch.
// Malformed poison-pill records are logged (redacted), counted, and skipped.
// Returns repository.ErrWALFull if downstream storage fails and the local WAL reaches its 5 GiB cap.
func (p *ProcessKafkaBatch) Handle(ctx context.Context, records []RawKafkaRecord) error {
	if len(records) == 0 {
		return nil
	}

	entries := make([]entity.ComplianceEntry, 0, len(records))
	tenantCounts := make(map[string]int)

	for i := range records {
		rec := records[i]
		var pb auditv1.AuditRecord
		if err := proto.Unmarshal(rec.Value, &pb); err != nil {
			p.metrics.IncMalformedRecords(1)
			p.logger.Warn("skipping malformed protobuf audit record",
				slog.Uint64("partition", uint64(rec.Partition)),
				slog.Uint64("offset", rec.Offset),
				slog.String("error", err.Error()),
			)
			continue
		}

		entry := protoToEntity(&pb, rec.Partition, rec.Offset)
		if err := entry.Validate(); err != nil {
			p.metrics.IncMalformedRecords(1)
			p.logger.Warn("skipping invalid audit record",
				slog.String("trace_id", entry.TraceID),
				slog.String("tenant_id", entry.TenantID),
				slog.Uint64("partition", uint64(rec.Partition)),
				slog.Uint64("offset", rec.Offset),
				slog.String("error", err.Error()),
			)
			continue
		}

		if len(p.producerTenants) > 0 {
			expectedTenant, ok := p.producerTenants[entry.ProducerID]
			if !ok || expectedTenant != entry.TenantID {
				p.metrics.IncMalformedRecords(1)
				p.logger.Warn("skipping audit record with untrusted producer-to-tenant binding",
					slog.String("trace_id", entry.TraceID),
					slog.String("producer_id", entry.ProducerID),
					slog.Uint64("partition", uint64(rec.Partition)),
					slog.Uint64("offset", rec.Offset),
				)
				continue
			}
		}

		if len(entry.PayloadRedacted) > 0 {
			entry.PayloadRedacted = p.redactor.RedactInPlace(entry.PayloadRedacted)
		}

		entries = append(entries, entry)
		tenantCounts[entry.TenantID]++
	}

	if len(entries) == 0 {
		return nil
	}

	for tenantID, count := range tenantCounts {
		p.metrics.IncRecordsConsumed(tenantID, count)
	}

	manifests := p.seqTrack.TrackBatch(entries)
	for i := range manifests {
		m := &manifests[i]
		if len(m.Gaps) > 0 {
			p.metrics.IncSequenceGaps(len(m.Gaps))
		}
		for _, g := range m.Gaps {
			p.logger.Warn("sequence gap detected in ingested batch",
				slog.String("tenant_id", g.Key.TenantID),
				slog.String("producer_id", g.Key.ProducerID),
				slog.Uint64("partition", uint64(g.Key.KafkaPartition)),
				slog.Uint64("min_seq", g.MinSeq),
				slog.Uint64("max_seq", g.MaxSeq),
			)
		}
	}

	var errs []error

	if p.hot != nil {
		if failedHot, err := p.hot.Add(ctx, entries); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failedHot, true, false); walErr != nil {
				return walErr
			}
		}
	}

	if p.cold != nil {
		if failedCold, err := p.cold.Add(ctx, entries); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failedCold, false, true); walErr != nil {
				return walErr
			}
		}
	}

	return errors.Join(errs...)
}

// FlushAll forces both hot and cold flushers to persist buffered records,
// spilling any failed batches to mmap_wal.
func (p *ProcessKafkaBatch) FlushAll(ctx context.Context) error {
	var errs []error
	if p.hot != nil {
		if failed, err := p.hot.Flush(ctx); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failed, true, false); walErr != nil {
				return walErr
			}
		}
	}
	if p.cold != nil {
		if failed, err := p.cold.Flush(ctx); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failed, false, true); walErr != nil {
				return walErr
			}
		}
	}
	return errors.Join(errs...)
}

// FlushPartition forces both hot and cold flushers to persist buffered records for partition.
func (p *ProcessKafkaBatch) FlushPartition(ctx context.Context, partition uint16) error {
	var errs []error
	if p.hot != nil {
		if failed, err := p.hot.FlushPartition(ctx, partition); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failed, true, false); walErr != nil {
				return walErr
			}
		}
	}
	if p.cold != nil {
		if failed, err := p.cold.FlushPartition(ctx, partition); err != nil {
			errs = append(errs, err)
			if walErr := p.spillToWAL(ctx, failed, false, true); walErr != nil {
				return walErr
			}
		}
	}
	return errors.Join(errs...)
}

// DrainWALOnce replays up to limit spooled records from mmap_wal into ClickHouse and S3,
// acknowledging records only after required downstream writes succeed.
func (p *ProcessKafkaBatch) DrainWALOnce(ctx context.Context, limit int) (int, error) {
	if p.wal == nil || !p.wal.IsSpilloverActive() {
		p.metrics.SetWALSpilloverActive(false)
		return 0, nil
	}

	records, err := p.wal.Drain(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		p.metrics.SetWALSpilloverActive(p.wal.IsSpilloverActive())
		return 0, nil
	}

	type decoded struct {
		walRec repository.WALRecord
		env    walEnvelope
	}
	items := make([]decoded, 0, len(records))
	var hotBatch []entity.ComplianceEntry
	coldGrouped := make(map[entity.StreamKey][]entity.ComplianceEntry)

	for i := range records {
		var env walEnvelope
		if uErr := json.Unmarshal(records[i].Payload, &env); uErr != nil {
			p.logger.Warn("skipping corrupt wal envelope",
				slog.Uint64("segment_id", records[i].SegmentID),
				slog.Int64("offset", records[i].Offset),
				slog.String("error", uErr.Error()),
			)
			if ackErr := p.wal.Ack(ctx, records[i].SegmentID, records[i].Offset); ackErr != nil {
				return 0, ackErr
			}
			continue
		}
		items = append(items, decoded{walRec: records[i], env: env})
		if env.NeedHot && p.hotStore != nil {
			hotBatch = append(hotBatch, env.Entry)
		}
		if env.NeedCold && p.coldStore != nil {
			k := entity.StreamKey{
				ProducerID:     env.Entry.ProducerID,
				TenantID:       env.Entry.TenantID,
				KafkaPartition: env.Entry.KafkaPartition,
			}
			coldGrouped[k] = append(coldGrouped[k], env.Entry)
		}
	}

	if len(hotBatch) > 0 {
		if err := p.hotStore.WriteBatch(ctx, hotBatch); err != nil {
			return 0, fmt.Errorf("wal replay clickhouse: %w", err)
		}
	}
	for k, list := range coldGrouped {
		minSeq, maxSeq := list[0].SeqID, list[0].SeqID
		for i := 1; i < len(list); i++ {
			if list[i].SeqID < minSeq {
				minSeq = list[i].SeqID
			}
			if list[i].SeqID > maxSeq {
				maxSeq = list[i].SeqID
			}
		}
		manifest := entity.BatchManifest{
			Key:         k,
			MinSeq:      minSeq,
			MaxSeq:      maxSeq,
			RecordCount: len(list),
		}
		if err := p.coldStore.WriteBatch(ctx, manifest, list); err != nil {
			return 0, fmt.Errorf("wal replay s3 worm: %w", err)
		}
	}

	for i := range items {
		if err := p.wal.Ack(ctx, items[i].walRec.SegmentID, items[i].walRec.Offset); err != nil {
			return i, err
		}
	}
	p.metrics.SetWALSpilloverActive(p.wal.IsSpilloverActive())
	return len(items), nil
}

func (p *ProcessKafkaBatch) spillToWAL(ctx context.Context, entries []entity.ComplianceEntry, needHot, needCold bool) error {
	if p.wal == nil || len(entries) == 0 {
		return nil
	}
	for i := range entries {
		payload, err := json.Marshal(walEnvelope{
			NeedHot:  needHot,
			NeedCold: needCold,
			Entry:    entries[i],
		})
		if err != nil {
			return fmt.Errorf("marshal wal envelope: %w", err)
		}
		if err := p.wal.Append(ctx, payload); err != nil {
			p.metrics.SetWALSpilloverActive(p.wal.IsSpilloverActive())
			return err
		}
	}
	p.metrics.SetWALSpilloverActive(p.wal.IsSpilloverActive())
	return nil
}

func protoToEntity(pb *auditv1.AuditRecord, partition uint16, offset uint64) entity.ComplianceEntry {
	var ts time.Time
	if pb.GetEventTime() != nil {
		ts = pb.GetEventTime().AsTime().UTC()
	}

	var actorID, authMethod, sourceIPHash string
	if pb.GetActor() != nil {
		actorID = pb.GetActor().GetActorId()
		authMethod = pb.GetActor().GetAuthMethod()
		sourceIPHash = pb.GetActor().GetSourceIpHash()
	}

	var docIDs []uint32
	var chunkIDs, docVersions []string
	var cacheHit bool
	var embedVer string
	if pb.GetAccessTrace() != nil {
		docIDs = pb.GetAccessTrace().GetDocIds()
		chunkIDs = pb.GetAccessTrace().GetChunkIds()
		docVersions = pb.GetAccessTrace().GetDocumentVersions()
		cacheHit = pb.GetAccessTrace().GetCacheHit()
		embedVer = pb.GetAccessTrace().GetEmbeddingModelVersion()
	}

	payloadCopy := make([]byte, len(pb.GetPayloadRedacted()))
	copy(payloadCopy, pb.GetPayloadRedacted())

	var plane entity.PlaneType
	switch pb.GetPlane() {
	case auditv1.PlaneType_PLANE_TYPE_READ:
		plane = entity.PlaneRead
	case auditv1.PlaneType_PLANE_TYPE_WRITE:
		plane = entity.PlaneWrite
	default:
		plane = entity.PlaneUnspecified
	}

	return entity.ComplianceEntry{
		EventID:               pb.GetEventId(),
		TraceID:               pb.GetTraceId(),
		TenantID:              pb.GetTenantId(),
		ProducerID:            pb.GetProducerId(),
		KafkaPartition:        partition,
		KafkaOffset:           offset,
		SeqID:                 pb.GetSeqId(),
		EventTime:             ts,
		Plane:                 plane,
		Action:                pb.GetAction(),
		ServiceName:           pb.GetServiceName(),
		ActorID:               actorID,
		AuthMethod:            authMethod,
		SourceIPHash:          sourceIPHash,
		DocIDs:                docIDs,
		ChunkIDs:              chunkIDs,
		DocumentVersions:      docVersions,
		CacheHit:              cacheHit,
		EmbeddingModelVersion: embedVer,
		PayloadRedacted:       payloadCopy,
		PromptTokens:          pb.GetPromptTokens(),
		CompletionTokens:      pb.GetCompletionTokens(),
	}
}
