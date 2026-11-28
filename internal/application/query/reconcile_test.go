package query

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

type mockHot struct {
	ranges []repository.SequenceRange
}

func (m *mockHot) WriteBatch(_ context.Context, _ []entity.ComplianceEntry) error { return nil }
func (m *mockHot) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return m.ranges, nil
}
func (m *mockHot) Ping(_ context.Context) error { return nil }
func (m *mockHot) Close() error                 { return nil }

type mockCold struct {
	manifests []repository.ColdObjectManifest
}

func (m *mockCold) WriteBatch(_ context.Context, _ entity.BatchManifest, _ []entity.ComplianceEntry) error {
	return nil
}
func (m *mockCold) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return m.manifests, nil
}
func (m *mockCold) Close() error { return nil }

func TestReconcilePartitionGaps_Consistent(t *testing.T) {
	hot := &mockHot{
		ranges: []repository.SequenceRange{
			{ProducerID: "prod-a", MinSeq: 100, MaxSeq: 120},
		},
	}
	cold := &mockCold{
		manifests: []repository.ColdObjectManifest{
			{Key: "k1", ProducerID: "prod-a", MinSeq: 100, MaxSeq: 110},
			{Key: "k2", ProducerID: "prod-a", MinSeq: 111, MaxSeq: 120},
		},
	}
	q := NewReconcilePartitionGaps(hot, cold, slog.New(slog.NewJSONHandler(io.Discard, nil)), observability.NewMetrics())
	res, err := q.Execute(context.Background(), ReconcileRequest{
		TenantID:  "tenant-1",
		Partition: 3,
		FromSeq:   100,
		ToSeq:     120,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != "consistent" || len(res.Gaps) != 0 || len(res.Overlaps) != 0 {
		t.Fatalf("expected consistent with 0 gaps/overlaps, got %+v", res)
	}
}

func TestReconcilePartitionGaps_DetectsMissingTiersAndOverlaps(t *testing.T) {
	hot := &mockHot{
		ranges: []repository.SequenceRange{
			{ProducerID: "prod-a", MinSeq: 100, MaxSeq: 105},
			{ProducerID: "prod-a", MinSeq: 108, MaxSeq: 112}, // 106-107 missing in both
		},
	}
	cold := &mockCold{
		manifests: []repository.ColdObjectManifest{
			{Key: "k1", ProducerID: "prod-a", MinSeq: 100, MaxSeq: 103}, // 104-105 missing in s3
			{Key: "k2", ProducerID: "prod-a", MinSeq: 102, MaxSeq: 103}, // overlaps k1 [102..103]
			{Key: "k3", ProducerID: "prod-a", MinSeq: 108, MaxSeq: 115}, // 113-115 missing in clickhouse
		},
	}
	q := NewReconcilePartitionGaps(hot, cold, slog.New(slog.NewJSONHandler(io.Discard, nil)), observability.NewMetrics())
	res, err := q.Execute(context.Background(), ReconcileRequest{
		TenantID:  "tenant-1",
		Partition: 3,
		FromSeq:   100,
		ToSeq:     115,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != "inconsistent" {
		t.Fatalf("expected inconsistent, got %s", res.Status)
	}
	if len(res.Overlaps) != 1 || res.Overlaps[0].MinSeq != 102 || res.Overlaps[0].MaxSeq != 103 {
		t.Fatalf("expected 1 overlap [102..103], got %+v", res.Overlaps)
	}
	if len(res.Gaps) != 3 {
		t.Fatalf("expected 3 gaps, got %+v", res.Gaps)
	}
	if res.Gaps[0].MinSeq != 104 || res.Gaps[0].MaxSeq != 105 || res.Gaps[0].MissingIn != "s3" {
		t.Fatalf("unexpected gap[0]: %+v", res.Gaps[0])
	}
	if res.Gaps[1].MinSeq != 106 || res.Gaps[1].MaxSeq != 107 || res.Gaps[1].MissingIn != "both" {
		t.Fatalf("unexpected gap[1]: %+v", res.Gaps[1])
	}
	if res.Gaps[2].MinSeq != 113 || res.Gaps[2].MaxSeq != 115 || res.Gaps[2].MissingIn != "clickhouse" {
		t.Fatalf("unexpected gap[2]: %+v", res.Gaps[2])
	}
}
