package pool

import "sync"

const (
	// DefaultInitialCap is the initial capacity of pooled byte slices (4 KiB).
	DefaultInitialCap = 4096
	// DefaultMaxCap is the maximum retained capacity of pooled byte slices (256 KiB)
	// to prevent oversized payloads from bloating container RSS.
	DefaultMaxCap = 256 * 1024
)

// BytePool manages reusable []byte buffers via sync.Pool with a capacity cap.
type BytePool struct {
	pool   sync.Pool
	maxCap int
}

// NewBytePool constructs a BytePool with the given initial and maximum retained capacities.
func NewBytePool(initialCap, maxCap int) *BytePool {
	if initialCap <= 0 {
		initialCap = DefaultInitialCap
	}
	if maxCap < initialCap {
		maxCap = initialCap
	}
	return &BytePool{
		maxCap: maxCap,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, initialCap)
				return &b
			},
		},
	}
}

// Get retrieves a reset ([]byte)[:0] buffer from the pool.
func (p *BytePool) Get() *[]byte {
	bp := p.pool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

// Put resets the buffer length to zero and returns it to the pool unless its
// capacity exceeds maxCap, in which case it is dropped for GC reclamation.
func (p *BytePool) Put(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) > p.maxCap {
		return
	}
	*bp = (*bp)[:0]
	p.pool.Put(bp)
}
