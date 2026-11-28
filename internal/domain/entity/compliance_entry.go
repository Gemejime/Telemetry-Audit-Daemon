package entity

import (
	"errors"
	"fmt"
	"time"
)

// PlaneType distinguishes non-blocking read-plane queries from fail-closed write-plane mutations.
type PlaneType int8

const (
	PlaneUnspecified PlaneType = 0
	PlaneRead        PlaneType = 1
	PlaneWrite       PlaneType = 2
)

var (
	// ErrInvalidEntry indicates a ComplianceEntry failed domain validation.
	ErrInvalidEntry = errors.New("invalid compliance entry")
)

// ComplianceEntry is the pure-Go domain representation of a redacted audit record
// enriched with Kafka consumer partition/offset metadata.
type ComplianceEntry struct {
	EventID               string
	TraceID               string
	TenantID              string
	ProducerID            string
	KafkaPartition        uint16
	KafkaOffset           uint64
	SeqID                 uint64
	EventTime             time.Time
	Plane                 PlaneType
	Action                string
	ServiceName           string
	ActorID               string
	AuthMethod            string
	SourceIPHash          string // 64-char hex string matching ClickHouse FixedString(64)
	DocIDs                []uint32
	ChunkIDs              []string
	DocumentVersions      []string
	CacheHit              bool
	EmbeddingModelVersion string
	PayloadRedacted       []byte
	PromptTokens          uint32
	CompletionTokens      uint32
}

// Validate enforces required domain invariants before persistence.
func (e *ComplianceEntry) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("%w: missing event_id", ErrInvalidEntry)
	}
	if e.TraceID == "" {
		return fmt.Errorf("%w: missing trace_id", ErrInvalidEntry)
	}
	if e.TenantID == "" {
		return fmt.Errorf("%w: missing tenant_id", ErrInvalidEntry)
	}
	if e.ProducerID == "" {
		return fmt.Errorf("%w: missing producer_id", ErrInvalidEntry)
	}
	if e.SeqID == 0 {
		return fmt.Errorf("%w: seq_id must be > 0", ErrInvalidEntry)
	}
	if e.EventTime.IsZero() {
		return fmt.Errorf("%w: missing event_time", ErrInvalidEntry)
	}
	if e.Plane != PlaneRead && e.Plane != PlaneWrite {
		return fmt.Errorf("%w: invalid plane %d", ErrInvalidEntry, e.Plane)
	}
	if e.Action == "" {
		return fmt.Errorf("%w: missing action", ErrInvalidEntry)
	}
	if e.ActorID == "" {
		return fmt.Errorf("%w: missing actor_id", ErrInvalidEntry)
	}
	if !isValidHex64(e.SourceIPHash) {
		return fmt.Errorf("%w: source_ip_hash must be 64 hex characters", ErrInvalidEntry)
	}
	return nil
}

func isValidHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}
