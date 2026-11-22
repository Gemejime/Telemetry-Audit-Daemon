package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"rag-telemetry-audit-daemon/internal/domain/repository"
)

const (
	// DefaultMaxWALBytes is the hard 5 GiB spillover cap on the 10 GiB emptyDir volume.
	DefaultMaxWALBytes int64 = 5 * 1024 * 1024 * 1024
	// DefaultSegmentBytes is the default 64 MiB segment rotation size.
	DefaultSegmentBytes int64 = 64 * 1024 * 1024

	frameHeaderSize = 8
	segFilePrefix   = "wal-"
	segFileSuffix   = ".seg"
)

type segmentBackend interface {
	WriteAt(p []byte, off int64) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	RefreshReadMap(size int64) error
	Truncate(size int64) error
	Sync() error
	Close() error
}

type walSegment struct {
	id          uint64
	path        string
	backend     segmentBackend
	writeOffset int64
	readOffset  int64
}

// MmapWAL implements repository.WALPort using bounded, rotated NVMe segment files
// with CRC32 record framing and a hard 5 GiB capacity ceiling.
type MmapWAL struct {
	mu           sync.Mutex
	dir          string
	maxBytes     int64
	segmentBytes int64
	totalBytes   int64
	segments     []*walSegment
	nextSegID    uint64
	logger       *slog.Logger
	closed       bool
}

// NewMmapWAL initializes or recovers a segmented WAL in dir.
func NewMmapWAL(dir string, maxBytes, segmentBytes int64, logger *slog.Logger) (*MmapWAL, error) {
	if dir == "" {
		return nil, errors.New("wal directory is required")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxWALBytes
	}
	if segmentBytes <= 0 {
		segmentBytes = DefaultSegmentBytes
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	w := &MmapWAL{
		dir:          dir,
		maxBytes:     maxBytes,
		segmentBytes: segmentBytes,
		nextSegID:    1,
		logger:       logger,
	}
	if err := w.recoverSegments(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *MmapWAL) recoverSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("read wal dir: %w", err)
	}

	type segInfo struct {
		id   uint64
		path string
		size int64
	}
	var found []segInfo
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasPrefix(name, segFilePrefix) || !strings.HasSuffix(name, segFileSuffix) {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, segFilePrefix), segFileSuffix)
		id, parseErr := strconv.ParseUint(idStr, 10, 64)
		if parseErr != nil {
			continue
		}
		info, statErr := ent.Info()
		if statErr != nil {
			return fmt.Errorf("stat wal segment: %w", statErr)
		}
		found = append(found, segInfo{
			id:   id,
			path: filepath.Join(w.dir, name),
			size: info.Size(),
		})
	}

	sort.Slice(found, func(i, j int) bool { return found[i].id < found[j].id })
	for _, sf := range found {
		backend, openErr := openSegmentBackend(sf.path, w.segmentBytes)
		if openErr != nil {
			_ = w.Close()
			return fmt.Errorf("open wal segment %d: %w", sf.id, openErr)
		}
		w.segments = append(w.segments, &walSegment{
			id:          sf.id,
			path:        sf.path,
			backend:     backend,
			writeOffset: sf.size,
		})
		w.totalBytes += sf.size
		if sf.id >= w.nextSegID {
			w.nextSegID = sf.id + 1
		}
	}
	return nil
}

// Append writes a CRC32-framed compliance record to the active WAL segment.
// Returns repository.ErrWALFull if appending would exceed maxBytes (5 GiB default).
func (w *MmapWAL) Append(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	frameLen := int64(frameHeaderSize + len(payload))

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal is closed")
	}
	if w.totalBytes+frameLen > w.maxBytes {
		return repository.ErrWALFull
	}

	seg, err := w.activeSegmentForWriteLocked(frameLen)
	if err != nil {
		return err
	}

	var hdr [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload))) // #nosec G115 -- payload bounded by segmentBytes/maxBytes
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))

	if _, err := seg.backend.WriteAt(hdr[:], seg.writeOffset); err != nil {
		return fmt.Errorf("write wal header: %w", err)
	}
	if _, err := seg.backend.WriteAt(payload, seg.writeOffset+frameHeaderSize); err != nil {
		return fmt.Errorf("write wal payload: %w", err)
	}

	seg.writeOffset += frameLen
	w.totalBytes += frameLen
	return nil
}

func (w *MmapWAL) activeSegmentForWriteLocked(frameLen int64) (*walSegment, error) {
	if len(w.segments) > 0 {
		last := w.segments[len(w.segments)-1]
		if last.writeOffset == 0 || last.writeOffset+frameLen <= w.segmentBytes {
			return last, nil
		}
	}

	id := w.nextSegID
	w.nextSegID++
	path := filepath.Join(w.dir, fmt.Sprintf("%s%010d%s", segFilePrefix, id, segFileSuffix))
	backend, err := openSegmentBackend(path, w.segmentBytes)
	if err != nil {
		return nil, fmt.Errorf("create wal segment %d: %w", id, err)
	}
	seg := &walSegment{
		id:      id,
		path:    path,
		backend: backend,
	}
	w.segments = append(w.segments, seg)
	return seg, nil
}

// Drain reads up to limit un-acknowledged records from the oldest WAL segments.
// Records remain in the WAL until Ack is called.
func (w *MmapWAL) Drain(ctx context.Context, limit int) ([]repository.WALRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 256
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, errors.New("wal is closed")
	}

	var records []repository.WALRecord
	var hdr [frameHeaderSize]byte

	for _, seg := range w.segments {
		if len(records) >= limit {
			break
		}
		if seg.readOffset >= seg.writeOffset {
			continue
		}
		if err := seg.backend.RefreshReadMap(seg.writeOffset); err != nil {
			return records, fmt.Errorf("refresh wal segment %d: %w", seg.id, err)
		}

		cursor := seg.readOffset
		for cursor < seg.writeOffset && len(records) < limit {
			if err := ctx.Err(); err != nil {
				return records, err
			}
			if cursor+frameHeaderSize > seg.writeOffset {
				w.logger.Warn("truncated wal frame header detected",
					slog.Uint64("segment_id", seg.id),
					slog.Int64("offset", cursor),
				)
				return records, repository.ErrWALCorrupt
			}
			if _, err := seg.backend.ReadAt(hdr[:], cursor); err != nil {
				return records, fmt.Errorf("read wal header: %w", err)
			}

			payloadLen := binary.LittleEndian.Uint32(hdr[0:4])
			expectedCRC := binary.LittleEndian.Uint32(hdr[4:8])
			nextOffset := cursor + frameHeaderSize + int64(payloadLen)
			if payloadLen == 0 || nextOffset > seg.writeOffset {
				w.logger.Warn("corrupt wal frame length detected",
					slog.Uint64("segment_id", seg.id),
					slog.Int64("offset", cursor),
				)
				return records, repository.ErrWALCorrupt
			}

			payload := make([]byte, payloadLen)
			if _, err := seg.backend.ReadAt(payload, cursor+frameHeaderSize); err != nil {
				return records, fmt.Errorf("read wal payload: %w", err)
			}
			if crc32.ChecksumIEEE(payload) != expectedCRC {
				w.logger.Warn("wal record crc32 mismatch",
					slog.Uint64("segment_id", seg.id),
					slog.Int64("offset", cursor),
				)
				return records, repository.ErrWALCorrupt
			}

			records = append(records, repository.WALRecord{
				SegmentID: seg.id,
				Offset:    nextOffset,
				Payload:   payload,
			})
			cursor = nextOffset
		}
	}

	return records, nil
}

// Ack advances the read cursor for segmentID and unmaps/removes fully-drained segments.
func (w *MmapWAL) Ack(ctx context.Context, segmentID uint64, offset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal is closed")
	}

	for _, seg := range w.segments {
		if seg.id == segmentID && offset > seg.readOffset {
			if offset > seg.writeOffset {
				offset = seg.writeOffset
			}
			seg.readOffset = offset
			break
		}
	}

	// Reclaim fully drained segments from the head of the queue.
	for len(w.segments) > 0 {
		head := w.segments[0]
		if head.readOffset < head.writeOffset {
			break
		}
		if len(w.segments) == 1 {
			// Reset the single active segment in-place so totalBytes drops to 0.
			if head.writeOffset > 0 {
				if err := head.backend.Truncate(0); err != nil {
					return fmt.Errorf("truncate drained wal segment %d: %w", head.id, err)
				}
				w.totalBytes -= head.writeOffset
				head.writeOffset = 0
				head.readOffset = 0
			}
			break
		}

		drainedBytes := head.writeOffset
		_ = head.backend.Close()
		if err := os.Remove(head.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove drained wal segment %d: %w", head.id, err)
		}
		w.totalBytes -= drainedBytes
		w.segments = w.segments[1:]
	}

	if w.totalBytes < 0 {
		w.totalBytes = 0
	}
	return nil
}

// ActiveBytes returns the total un-drained bytes currently stored in the WAL.
func (w *MmapWAL) ActiveBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalBytes
}

// IsSpilloverActive reports whether any un-drained compliance records are spooled on disk.
func (w *MmapWAL) IsSpilloverActive() bool {
	return w.ActiveBytes() > 0
}

// Close syncs and closes all open WAL segment backends.
func (w *MmapWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	var firstErr error
	for _, seg := range w.segments {
		if err := seg.backend.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := seg.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.segments = nil
	return firstErr
}
