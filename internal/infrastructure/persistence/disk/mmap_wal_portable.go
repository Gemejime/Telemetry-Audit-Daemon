//go:build !linux

package disk

import "os"

type portableFileSegment struct {
	file *os.File
}

func openSegmentBackend(path string, _ int64) (segmentBackend, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- path constructed internally from WAL dir and uint64 segment ID
	if err != nil {
		return nil, err
	}
	return &portableFileSegment{file: f}, nil
}

func (s *portableFileSegment) WriteAt(p []byte, off int64) (int, error) {
	return s.file.WriteAt(p, off)
}

func (s *portableFileSegment) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

func (s *portableFileSegment) RefreshReadMap(_ int64) error {
	return nil
}

func (s *portableFileSegment) Truncate(size int64) error {
	return s.file.Truncate(size)
}

func (s *portableFileSegment) Sync() error {
	return s.file.Sync()
}

func (s *portableFileSegment) Close() error {
	return s.file.Close()
}
