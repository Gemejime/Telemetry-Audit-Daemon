package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"rag-telemetry-audit-daemon/internal/domain/repository"
)

func TestMmapWAL_AppendDrainAckAndRotation(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Small segment & max cap to exercise rotation and ErrWALFull deterministically.
	wal, err := NewMmapWAL(dir, 256, 64, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	ctx := context.Background()
	p1 := bytes.Repeat([]byte("a"), 40)
	p2 := bytes.Repeat([]byte("b"), 40)
	p3 := bytes.Repeat([]byte("c"), 40)

	for _, p := range [][]byte{p1, p2, p3} {
		if err := wal.Append(ctx, p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if !wal.IsSpilloverActive() {
		t.Fatal("expected spillover active")
	}

	// Verify 3 segments were created due to 64-byte segment cap (40 + 8 = 48 bytes each).
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(entries))
	}

	recs, err := wal.Drain(ctx, 10)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	if !bytes.Equal(recs[0].Payload, p1) || !bytes.Equal(recs[1].Payload, p2) || !bytes.Equal(recs[2].Payload, p3) {
		t.Fatal("drained payloads mismatch")
	}

	// Ack all records and verify drained segments are removed and spillover becomes inactive.
	for _, r := range recs {
		if err := wal.Ack(ctx, r.SegmentID, r.Offset); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if wal.IsSpilloverActive() || wal.ActiveBytes() != 0 {
		t.Fatalf("expected 0 active bytes after ack, got %d", wal.ActiveBytes())
	}
}

func TestMmapWAL_EnforcesMaxCapErrWALFull(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewMmapWAL(dir, 60, 60, nil)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	ctx := context.Background()
	if err := wal.Append(ctx, bytes.Repeat([]byte("x"), 30)); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	// 38 bytes used; appending 30+8=38 bytes exceeds 60 byte cap.
	err = wal.Append(ctx, bytes.Repeat([]byte("y"), 30))
	if !errors.Is(err, repository.ErrWALFull) {
		t.Fatalf("expected ErrWALFull, got %v", err)
	}
}

func TestMmapWAL_DetectsCRC32Corruption(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewMmapWAL(dir, 1024, 512, nil)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}

	ctx := context.Background()
	if err := wal.Append(ctx, []byte("valid-compliance-frame")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = wal.Close()

	// Flip a byte in the payload of segment 1.
	segPath := filepath.Join(dir, "wal-0000000001.seg")
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(segPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	wal2, err := NewMmapWAL(dir, 1024, 512, nil)
	if err != nil {
		t.Fatalf("reopen NewMmapWAL: %v", err)
	}
	defer func() { _ = wal2.Close() }()

	_, err = wal2.Drain(ctx, 10)
	if !errors.Is(err, repository.ErrWALCorrupt) {
		t.Fatalf("expected ErrWALCorrupt, got %v", err)
	}
}
