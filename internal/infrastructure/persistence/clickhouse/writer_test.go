package clickhouse

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"rag-telemetry-audit-daemon/internal/domain/entity"
)

type mockBatch struct {
	driver.Batch
	rows    [][]any
	sent    bool
	aborted bool
	sendErr error
}

func (m *mockBatch) Abort() error { m.aborted = true; return nil }
func (m *mockBatch) Append(v ...any) error {
	m.rows = append(m.rows, v)
	return nil
}
func (m *mockBatch) IsSent() bool { return m.sent }
func (m *mockBatch) Rows() int    { return len(m.rows) }
func (m *mockBatch) Send() error  { m.sent = true; return m.sendErr }

type mockConn struct {
	prepareCalls int
	failUntil    int
	lastQuery    string
	lastBatch    *mockBatch
}

func (m *mockConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.prepareCalls++
	m.lastQuery = query
	b := &mockBatch{}
	if m.prepareCalls <= m.failUntil {
		b.sendErr = errors.New("transient tcp reset")
	}
	m.lastBatch = b
	return b, nil
}
func (m *mockConn) Ping(_ context.Context) error { return nil }
func (m *mockConn) Close() error                 { return nil }

type mockMetrics struct {
	calls int
}

func (m *mockMetrics) ObserveHotPersistDuration(_ time.Duration) {
	m.calls++
}

func validTestEntry(seq uint64) entity.ComplianceEntry {
	return entity.ComplianceEntry{
		EventID:               "01JTESTEVENT00000000000001",
		TraceID:               "01JTESTTRACE00000000000001",
		TenantID:              "tenant-corp-01",
		ProducerID:            "search-orch-1",
		KafkaPartition:        3,
		KafkaOffset:           900 + seq,
		SeqID:                 seq,
		EventTime:             time.Unix(1700000000, 0).UTC(),
		Plane:                 entity.PlaneWrite,
		Action:                "document.upload",
		ServiceName:           "rag-kb-catalog-service",
		ActorID:               "user-42",
		AuthMethod:            "oidc",
		SourceIPHash:          strings.Repeat("a", 64),
		DocIDs:                []uint32{101, 102},
		ChunkIDs:              []string{"c1", "c2"},
		DocumentVersions:      []string{"v1"},
		CacheHit:              false,
		EmbeddingModelVersion: "text-embedding-004",
		PayloadRedacted:       []byte(`{"email":"[REDACTED]"}`),
		PromptTokens:          120,
		CompletionTokens:      45,
	}
}

func TestRowBinaryWriter_ColumnMappingAndRetries(t *testing.T) {
	conn := &mockConn{failUntil: 2}
	metrics := &mockMetrics{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	writer, err := NewRowBinaryWriter(conn, RetryConfig{
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}, logger, metrics)
	if err != nil {
		t.Fatalf("NewRowBinaryWriter: %v", err)
	}

	entries := []entity.ComplianceEntry{validTestEntry(10), validTestEntry(11)}
	if err := writer.WriteBatch(context.Background(), entries); err != nil {
		t.Fatalf("WriteBatch failed after transient retries: %v", err)
	}

	if conn.prepareCalls != 3 {
		t.Fatalf("expected 3 prepare calls (2 retries + 1 success), got %d", conn.prepareCalls)
	}
	if metrics.calls != 1 {
		t.Fatalf("expected 1 metrics observation, got %d", metrics.calls)
	}
	if !strings.Contains(conn.lastQuery, "INSERT INTO audit_compliance_records") {
		t.Fatalf("unexpected query: %s", conn.lastQuery)
	}
	if strings.Contains(conn.lastQuery, "ingested_at") {
		t.Fatal("ingested_at should be omitted so ClickHouse populates DEFAULT now64(3)")
	}
	if len(conn.lastBatch.rows) != 2 {
		t.Fatalf("expected 2 rows appended, got %d", len(conn.lastBatch.rows))
	}
	// Verify column count (22 explicit columns) and Enum8 plane value.
	row0 := conn.lastBatch.rows[0]
	if len(row0) != 22 {
		t.Fatalf("expected 22 columns, got %d", len(row0))
	}
	if planeVal, ok := row0[8].(int8); !ok || planeVal != int8(entity.PlaneWrite) {
		t.Fatalf("expected int8(PlaneWrite) at col 8, got %T (%v)", row0[8], row0[8])
	}
}

func TestRowBinaryWriter_RejectsInvalidEntryBeforeIO(t *testing.T) {
	conn := &mockConn{}
	writer, err := NewRowBinaryWriter(conn, RetryConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRowBinaryWriter: %v", err)
	}

	bad := validTestEntry(1)
	bad.SourceIPHash = "invalid"
	if err := writer.WriteBatch(context.Background(), []entity.ComplianceEntry{bad}); err == nil {
		t.Fatal("expected validation error")
	}
	if conn.prepareCalls != 0 {
		t.Fatalf("expected 0 PrepareBatch calls on validation failure, got %d", conn.prepareCalls)
	}
}

type mockRows struct {
	driver.Rows
	data []struct {
		producer string
		seq      uint64
	}
	idx int
}

func (r *mockRows) Next() bool {
	if r.idx >= len(r.data) {
		return false
	}
	r.idx++
	return true
}

func (r *mockRows) Scan(dest ...any) error {
	row := r.data[r.idx-1]
	*(dest[0].(*string)) = row.producer
	*(dest[1].(*uint64)) = row.seq
	return nil
}

func (r *mockRows) Err() error   { return nil }
func (r *mockRows) Close() error { return nil }

type mockQueryConn struct {
	mockConn
	rows      driver.Rows
	lastQuery string
}

func (m *mockQueryConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	m.lastQuery = query
	return m.rows, nil
}

func TestRowBinaryWriter_QuerySequenceRanges_CoalescesAndDetectsGaps(t *testing.T) {
	rows := &mockRows{
		data: []struct {
			producer string
			seq      uint64
		}{
			{"prod-a", 10},
			{"prod-a", 11},
			{"prod-a", 12},
			{"prod-a", 15}, // gap 13..14
			{"prod-a", 16},
			{"prod-b", 1},
			{"prod-b", 2},
		},
	}
	conn := &mockQueryConn{rows: rows}
	writer, err := NewRowBinaryWriter(conn, RetryConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("NewRowBinaryWriter: %v", err)
	}

	got, err := writer.QuerySequenceRanges(context.Background(), "tenant-1", 2, 1, 20)
	if err != nil {
		t.Fatalf("QuerySequenceRanges: %v", err)
	}
	if !strings.Contains(conn.lastQuery, "FINAL") {
		t.Fatalf("expected FINAL in query, got %s", conn.lastQuery)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 coalesced ranges, got %d: %+v", len(got), got)
	}
	if got[0].ProducerID != "prod-a" || got[0].MinSeq != 10 || got[0].MaxSeq != 12 {
		t.Fatalf("unexpected range[0]: %+v", got[0])
	}
	if got[1].ProducerID != "prod-a" || got[1].MinSeq != 15 || got[1].MaxSeq != 16 {
		t.Fatalf("unexpected range[1]: %+v", got[1])
	}
	if got[2].ProducerID != "prod-b" || got[2].MinSeq != 1 || got[2].MaxSeq != 2 {
		t.Fatalf("unexpected range[2]: %+v", got[2])
	}
}
