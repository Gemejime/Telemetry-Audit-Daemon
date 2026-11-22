package http

import (
	"context"
	"encoding/json"
	"net/http"
)

// ReadinessChecker provides live health signals for /ready.
type ReadinessChecker interface {
	IsDraining() bool
	IsKafkaHealthy() bool
	PingClickHouse(ctx context.Context) error
	IsWALSpilloverActive() bool
}

// HealthHandler serves /healthz and /ready probes.
type HealthHandler struct {
	checker ReadinessChecker
}

// NewHealthHandler constructs a HealthHandler.
func NewHealthHandler(checker ReadinessChecker) *HealthHandler {
	return &HealthHandler{checker: checker}
}

// Healthz responds with 200 OK when the daemon process is alive.
func (h *HealthHandler) Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// Ready responds with 200 OK when Kafka consumer and ClickHouse are healthy and not draining,
// or 503 Service Unavailable during SIGTERM drain or dependency failure.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.checker == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ready",
			"checks": map[string]any{
				"kafka_consumer":       "ok",
				"clickhouse":           "ok",
				"wal_spillover_active": false,
			},
		})
		return
	}

	if h.checker.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "draining",
		})
		return
	}

	kafkaOK := h.checker.IsKafkaHealthy()
	kafkaStatus := "ok"
	if !kafkaOK {
		kafkaStatus = "unhealthy"
	}

	chStatus := "ok"
	chOK := true
	if err := h.checker.PingClickHouse(r.Context()); err != nil {
		chOK = false
		chStatus = "unhealthy"
	}

	walActive := h.checker.IsWALSpilloverActive()
	checks := map[string]any{
		"kafka_consumer":       kafkaStatus,
		"clickhouse":           chStatus,
		"wal_spillover_active": walActive,
	}

	if !kafkaOK || !chOK {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not_ready",
			"checks": checks,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
		"checks": checks,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
