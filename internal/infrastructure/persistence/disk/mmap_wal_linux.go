//go:build linux

package disk

import (
	"fmt"
	"os"
	"syscall"
)

type linuxMmapSegment struct {
	file       *os.File
	mapped     []byte
	mappedSize int64
}

func openSegmentBackend(path string, _ int64) (segmentBackend, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- path constructed internally from WAL dir and uint64 segment ID
	if err != nil {
		return nil, err
	}
	return &linuxMmapSegment{file: f}, nil
}

func (s *linuxMmapSegment) WriteAt(p []byte, off int64) (int, error) {
	return s.file.WriteAt(p, off)
}

func (s *linuxMmapSegment) ReadAt(p []byte, off int64) (int, error) {
	if s.mapped != nil && off+int64(len(p)) <= s.mappedSize {
		n := copy(p, s.mapped[off:off+int64(len(p))])
		return n, nil
	}
	return s.file.ReadAt(p, off)
}

func (s *linuxMmapSegment) RefreshReadMap(size int64) error {
	if size <= 0 || size == s.mappedSize {
		return nil
	}
	if s.mapped != nil {
		_ = syscall.Munmap(s.mapped)
		s.mapped = nil
		s.mappedSize = 0
	}
	mapped, err := syscall.Mmap(int(s.file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) // #nosec G115 -- segment size bounded by segmentBytes
	if err != nil {
		return fmt.Errorf("syscall.Mmap: %w", err)
	}
	s.mapped = mapped
	s.mappedSize = size
	return nil
}

func (s *linuxMmapSegment) Truncate(size int64) error {
	if s.mapped != nil {
		_ = syscall.Munmap(s.mapped)
		s.mapped = nil
		s.mappedSize = 0
	}
	return s.file.Truncate(size)
}

func (s *linuxMmapSegment) Sync() error {
	return s.file.Sync()
}

func (s *linuxMmapSegment) Close() error {
	var firstErr error
	if s.mapped != nil {
		if err := syscall.Munmap(s.mapped); err != nil {
			firstErr = err
		}
		s.mapped = nil
		s.mappedSize = 0
	}
	if err := s.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
