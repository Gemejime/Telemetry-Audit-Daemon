package redaction

import "rag-telemetry-audit-daemon/pkg/pool"

var redactedTag = []byte("[REDACTED]")

// Engine performs amortized zero-allocation PII/PHI and secret redaction directly
// over []byte slices using a compiled Aho-Corasick automaton, ASCII byte scanners,
// and a sync.Pool-backed byte buffer pool.
type Engine struct {
	ac   *automaton
	pool *pool.BytePool
}

// NewEngine constructs a redaction Engine using the provided BytePool.
// If bp is nil, a default BytePool is created.
func NewEngine(bp *pool.BytePool) *Engine {
	if bp == nil {
		bp = pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	}
	return &Engine{
		ac:   newAutomaton(defaultKeyPatterns),
		pool: bp,
	}
}

// RedactInto scans src for PII/PHI and credentials, appending the sanitized bytes
// into dst[:0] and returning the resulting slice. When dst has sufficient capacity,
// RedactInto performs zero heap allocations.
func (e *Engine) RedactInto(dst, src []byte) []byte {
	dst = dst[:0]
	if len(src) == 0 {
		return dst
	}

	var state int32
	lastCopy := 0
	i := 0

	for i < len(src) {
		b := src[i]

		// 1. Advance Aho-Corasick keyword automaton.
		state = e.ac.step(state, b)
		if nd := e.ac.nodes[state]; nd.kind != kindNone {
			matchStart := i + 1 - int(nd.matchLen)
			if matchStart >= lastCopy {
				if valStart, valEnd, ok := scanSecretAfterKey(src, matchStart, i+1, nd.kind); ok {
					dst = append(dst, src[lastCopy:valStart]...)
					dst = append(dst, redactedTag...)
					lastCopy = valEnd
					i = valEnd
					state = 0
					continue
				}
			}
		}

		// 2. Email scanner triggered at '@' (backward + forward scan).
		if b == '@' && i > lastCopy {
			if start, end, ok := scanEmail(src, i, lastCopy); ok {
				dst = append(dst, src[lastCopy:start]...)
				dst = append(dst, redactedTag...)
				lastCopy = end
				i = end
				state = 0
				continue
			}
		}

		// 3. JWT scanner triggered at 'e' ("eyJ...").
		if b == 'e' && i >= lastCopy {
			if end, ok := scanJWT(src, i); ok {
				dst = append(dst, src[lastCopy:i]...)
				dst = append(dst, redactedTag...)
				lastCopy = end
				i = end
				state = 0
				continue
			}
		}

		// 4. Numeric & formatted PII scanners (SSN, Credit Card, IPv4, Phone).
		if i >= lastCopy && (isDigit(b) || b == '+' || b == '(') {
			if isDigit(b) {
				if end, ok := scanSSN(src, i); ok {
					dst = append(dst, src[lastCopy:i]...)
					dst = append(dst, redactedTag...)
					lastCopy = end
					i = end
					state = 0
					continue
				}
				if end, ok := scanCreditCard(src, i); ok {
					dst = append(dst, src[lastCopy:i]...)
					dst = append(dst, redactedTag...)
					lastCopy = end
					i = end
					state = 0
					continue
				}
				if end, ok := scanIPv4(src, i); ok {
					dst = append(dst, src[lastCopy:i]...)
					dst = append(dst, redactedTag...)
					lastCopy = end
					i = end
					state = 0
					continue
				}
			}
			if end, ok := scanPhone(src, i); ok {
				dst = append(dst, src[lastCopy:i]...)
				dst = append(dst, redactedTag...)
				lastCopy = end
				i = end
				state = 0
				continue
			}
		}

		i++
	}

	if lastCopy < len(src) {
		dst = append(dst, src[lastCopy:]...)
	}
	return dst
}

// RedactInPlace redacts buf using a pooled scratch buffer from the engine's BytePool
// and copies the sanitized result back into buf, returning the resized slice.
func (e *Engine) RedactInPlace(buf []byte) []byte {
	if len(buf) == 0 {
		return buf
	}
	scratchPtr := e.pool.Get()
	*scratchPtr = e.RedactInto(*scratchPtr, buf)
	resLen := len(*scratchPtr)
	if cap(buf) >= resLen {
		buf = buf[:resLen]
		copy(buf, *scratchPtr)
	} else {
		buf = append(buf[:0], *scratchPtr...)
	}
	e.pool.Put(scratchPtr)
	return buf
}

// WithRedacted redacts src into a pooled buffer and passes it to fn before returning
// the buffer to the pool. Callers must not retain the slice after fn returns.
func (e *Engine) WithRedacted(src []byte, fn func(redacted []byte) error) error {
	bp := e.pool.Get()
	*bp = e.RedactInto(*bp, src)
	err := fn(*bp)
	e.pool.Put(bp)
	return err
}
