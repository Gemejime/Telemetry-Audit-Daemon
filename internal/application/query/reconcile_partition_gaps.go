package query

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"

	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

// ReconcileRequest defines parameters for cross-tier [min_seq, max_seq] reconciliation.
type ReconcileRequest struct {
	TenantID  string
	Partition uint16
	FromSeq   uint64
	ToSeq     uint64
}

// ReconcileRange represents an inclusive [MinSeq, MaxSeq] sequence interval.
type ReconcileRange struct {
	MinSeq uint64
	MaxSeq uint64
}

// ReconcileGap describes a missing sequence range for a producer in one or both tiers.
type ReconcileGap struct {
	ProducerID string
	MinSeq     uint64
	MaxSeq     uint64
	MissingIn  string // "s3", "clickhouse", or "both"
}

// S3IntervalOverlap records an overlapping [min_seq, max_seq] range across two S3 Parquet objects.
type S3IntervalOverlap struct {
	ProducerID string
	KeyA       string
	KeyB       string
	MinSeq     uint64
	MaxSeq     uint64
}

// ReconcileResponse is the output of ReconcilePartitionGaps.
type ReconcileResponse struct {
	TenantID     string
	Partition    uint16
	CheckedRange ReconcileRange
	Gaps         []ReconcileGap
	Overlaps     []S3IntervalOverlap
	Status       string // "consistent" or "inconsistent"
}

// ReconcilePartitionGaps compares ClickHouse FINAL-deduplicated sequence ranges against
// S3 WORM Parquet footer manifests for a given (tenant_id, partition, [from_seq, to_seq]).
type ReconcilePartitionGaps struct {
	hotStore  repository.HotStorePort
	coldStore repository.ColdStorePort
	logger    *slog.Logger
	metrics   *observability.Metrics
}

// NewReconcilePartitionGaps constructs the cross-tier gap reconciliation query handler.
func NewReconcilePartitionGaps(
	hotStore repository.HotStorePort,
	coldStore repository.ColdStorePort,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *ReconcilePartitionGaps {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &ReconcilePartitionGaps{
		hotStore:  hotStore,
		coldStore: coldStore,
		logger:    logger,
		metrics:   metrics,
	}
}

// Execute runs cross-tier reconciliation and returns detected gaps and S3 interval overlaps.
func (q *ReconcilePartitionGaps) Execute(ctx context.Context, req ReconcileRequest) (ReconcileResponse, error) {
	if req.TenantID == "" {
		return ReconcileResponse{}, errors.New("tenant_id is required")
	}
	if req.FromSeq > req.ToSeq {
		return ReconcileResponse{}, errors.New("from_seq must be <= to_seq")
	}

	hotRanges, err := q.hotStore.QuerySequenceRanges(ctx, req.TenantID, req.Partition, req.FromSeq, req.ToSeq)
	if err != nil {
		return ReconcileResponse{}, err
	}
	coldManifests, err := q.coldStore.ListManifests(ctx, req.TenantID, req.Partition, req.FromSeq, req.ToSeq)
	if err != nil {
		return ReconcileResponse{}, err
	}

	hotByProducer := make(map[string][]seqSpan)
	producersSet := make(map[string]struct{})
	for _, r := range hotRanges {
		producersSet[r.ProducerID] = struct{}{}
		if span, ok := clampSpan(r.MinSeq, r.MaxSeq, req.FromSeq, req.ToSeq); ok {
			hotByProducer[r.ProducerID] = append(hotByProducer[r.ProducerID], span)
		}
	}

	coldRawByProducer := make(map[string][]repository.ColdObjectManifest)
	coldByProducer := make(map[string][]seqSpan)
	for _, m := range coldManifests {
		producersSet[m.ProducerID] = struct{}{}
		coldRawByProducer[m.ProducerID] = append(coldRawByProducer[m.ProducerID], m)
		if span, ok := clampSpan(m.MinSeq, m.MaxSeq, req.FromSeq, req.ToSeq); ok {
			coldByProducer[m.ProducerID] = append(coldByProducer[m.ProducerID], span)
		}
	}

	var overlaps []S3IntervalOverlap
	for prod, list := range coldRawByProducer {
		sort.Slice(list, func(i, j int) bool {
			if list[i].MinSeq != list[j].MinSeq {
				return list[i].MinSeq < list[j].MinSeq
			}
			return list[i].MaxSeq < list[j].MaxSeq
		})
		for i := 1; i < len(list); i++ {
			prev := list[i-1]
			curr := list[i]
			if curr.MinSeq <= prev.MaxSeq {
				ovMax := curr.MaxSeq
				if prev.MaxSeq < ovMax {
					ovMax = prev.MaxSeq
				}
				overlaps = append(overlaps, S3IntervalOverlap{
					ProducerID: prod,
					KeyA:       prev.Key,
					KeyB:       curr.Key,
					MinSeq:     curr.MinSeq,
					MaxSeq:     ovMax,
				})
				q.metrics.IncS3IntervalOverlaps(1)
				q.logger.Warn("overlapping s3 worm parquet intervals detected",
					slog.String("tenant_id", req.TenantID),
					slog.String("producer_id", prod),
					slog.Uint64("partition", uint64(req.Partition)),
					slog.Uint64("min_seq", curr.MinSeq),
					slog.Uint64("max_seq", ovMax),
				)
			}
		}
	}

	producers := make([]string, 0, len(producersSet))
	for p := range producersSet {
		producers = append(producers, p)
	}
	sort.Strings(producers)

	var gaps []ReconcileGap
	for _, prod := range producers {
		hotMerged := mergeSpans(hotByProducer[prod])
		coldMerged := mergeSpans(coldByProducer[prod])

		// 1. Present in ClickHouse, missing in S3.
		for _, g := range subtractSpans(hotMerged, coldMerged) {
			gaps = append(gaps, ReconcileGap{
				ProducerID: prod,
				MinSeq:     g.min,
				MaxSeq:     g.max,
				MissingIn:  "s3",
			})
		}
		// 2. Present in S3, missing in ClickHouse.
		for _, g := range subtractSpans(coldMerged, hotMerged) {
			gaps = append(gaps, ReconcileGap{
				ProducerID: prod,
				MinSeq:     g.min,
				MaxSeq:     g.max,
				MissingIn:  "clickhouse",
			})
		}
		// 3. Internal holes in the union of both tiers within [FromSeq, ToSeq].
		union := mergeSpans(append(append([]seqSpan(nil), hotMerged...), coldMerged...))
		for i := 1; i < len(union); i++ {
			gaps = append(gaps, ReconcileGap{
				ProducerID: prod,
				MinSeq:     union[i-1].max + 1,
				MaxSeq:     union[i].min - 1,
				MissingIn:  "both",
			})
		}
	}

	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].ProducerID != gaps[j].ProducerID {
			return gaps[i].ProducerID < gaps[j].ProducerID
		}
		if gaps[i].MinSeq != gaps[j].MinSeq {
			return gaps[i].MinSeq < gaps[j].MinSeq
		}
		return gaps[i].MaxSeq < gaps[j].MaxSeq
	})

	status := "consistent"
	if len(gaps) > 0 {
		status = "inconsistent"
		q.metrics.IncSequenceGaps(len(gaps))
	}

	return ReconcileResponse{
		TenantID:     req.TenantID,
		Partition:    req.Partition,
		CheckedRange: ReconcileRange{MinSeq: req.FromSeq, MaxSeq: req.ToSeq},
		Gaps:         gaps,
		Overlaps:     overlaps,
		Status:       status,
	}, nil
}

type seqSpan struct {
	min uint64
	max uint64
}

func clampSpan(minSeq, maxSeq, fromSeq, toSeq uint64) (seqSpan, bool) {
	if maxSeq < fromSeq || minSeq > toSeq {
		return seqSpan{}, false
	}
	if minSeq < fromSeq {
		minSeq = fromSeq
	}
	if maxSeq > toSeq {
		maxSeq = toSeq
	}
	return seqSpan{min: minSeq, max: maxSeq}, true
}

func mergeSpans(spans []seqSpan) []seqSpan {
	if len(spans) == 0 {
		return nil
	}
	sorted := make([]seqSpan, len(spans))
	copy(sorted, spans)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].min != sorted[j].min {
			return sorted[i].min < sorted[j].min
		}
		return sorted[i].max < sorted[j].max
	})

	merged := []seqSpan{sorted[0]}
	for i := 1; i < len(sorted); i++ {
		last := &merged[len(merged)-1]
		if sorted[i].min <= last.max+1 {
			if sorted[i].max > last.max {
				last.max = sorted[i].max
			}
		} else {
			merged = append(merged, sorted[i])
		}
	}
	return merged
}

// subtractSpans returns all sub-intervals of a not covered by b (both must be sorted and merged).
func subtractSpans(a, b []seqSpan) []seqSpan {
	if len(a) == 0 {
		return nil
	}
	if len(b) == 0 {
		return a
	}

	var out []seqSpan
	j := 0
	for _, iv := range a {
		cur := iv.min
		for j < len(b) && b[j].max < cur {
			j++
		}
		for k := j; k < len(b) && b[k].min <= iv.max; k++ {
			if b[k].min > cur {
				out = append(out, seqSpan{min: cur, max: b[k].min - 1})
			}
			if b[k].max >= iv.max {
				cur = iv.max + 1
				break
			}
			cur = b[k].max + 1
		}
		if cur <= iv.max {
			out = append(out, seqSpan{min: cur, max: iv.max})
		}
	}
	return out
}
