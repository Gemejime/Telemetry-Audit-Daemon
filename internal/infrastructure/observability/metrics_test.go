package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_BoundedTenantCardinality(t *testing.T) {
	m := NewMetricsWithTenantAllowlist([]string{"tenant-vip"})
	m.IncRecordsConsumed("tenant-vip", 3)
	m.IncRecordsConsumed("tenant-arbitrary-1", 4)
	m.IncRecordsConsumed("tenant-arbitrary-2", 5)

	if got := testutil.ToFloat64(m.RecordsConsumedTotal.WithLabelValues("tenant-vip")); got != 3 {
		t.Fatalf("expected 3 for allowlisted tenant-vip, got %v", got)
	}
	if got := testutil.ToFloat64(m.RecordsConsumedTotal.WithLabelValues(overflowTenantLabel)); got != 9 {
		t.Fatalf("expected 9 aggregated under %q, got %v", overflowTenantLabel, got)
	}
	if cnt := testutil.CollectAndCount(m.RecordsConsumedTotal); cnt != 2 {
		t.Fatalf("expected exactly 2 time series (tenant-vip + other), got %d", cnt)
	}
}
