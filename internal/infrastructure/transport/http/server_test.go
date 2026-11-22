package http

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"rag-telemetry-audit-daemon/internal/application/query"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

type stubReadiness struct {
	draining bool
	kafkaOK  bool
	chErr    error
	wal      bool
}

func (s *stubReadiness) IsDraining() bool                       { return s.draining }
func (s *stubReadiness) IsKafkaHealthy() bool                   { return s.kafkaOK }
func (s *stubReadiness) PingClickHouse(_ context.Context) error { return s.chErr }
func (s *stubReadiness) IsWALSpilloverActive() bool             { return s.wal }

type stubHot struct{}

func (stubHot) WriteBatch(_ context.Context, _ []entity.ComplianceEntry) error { return nil }
func (stubHot) QuerySequenceRanges(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.SequenceRange, error) {
	return []repository.SequenceRange{{ProducerID: "p1", MinSeq: 100, MaxSeq: 105}}, nil
}
func (stubHot) Ping(_ context.Context) error { return nil }
func (stubHot) Close() error                 { return nil }

type stubCold struct{}

func (stubCold) WriteBatch(_ context.Context, _ entity.BatchManifest, _ []entity.ComplianceEntry) error {
	return nil
}
func (stubCold) ListManifests(_ context.Context, _ string, _ uint16, _, _ uint64) ([]repository.ColdObjectManifest, error) {
	return []repository.ColdObjectManifest{{Key: "k1", ProducerID: "p1", MinSeq: 100, MaxSeq: 104}}, nil
}
func (stubCold) Close() error { return nil }

func signTestJWT(t *testing.T, secret string, claims authClaims) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(hdr + "." + payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hdr + "." + payload + "." + sig
}

func TestHealthReadyMetricsAndDocs(t *testing.T) {
	metrics := observability.NewMetrics()
	metrics.IncRecordsConsumed("tenant-1", 5)

	rc := &stubReadiness{kafkaOK: true}
	srv := NewServer(ServerConfig{
		OpenAPISpecYAML: []byte("openapi: 3.1.0\n"),
		HealthChecker:   rc,
		Metrics:         metrics,
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /healthz, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /ready, got %d", rec.Code)
	}

	rc.draining = true
	req = httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 /ready when draining, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "audit_records_consumed_total") {
		t.Fatalf("unexpected /metrics response: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "3.1.0") {
		t.Fatalf("unexpected /openapi.yaml response: %d", rec.Code)
	}
}

func TestReconcileGapsEndpoint_AuthTenantIsolationAndRateLimit(t *testing.T) {
	const secret = "test-hmac-secret-key"
	metrics := observability.NewMetrics()
	reconciler := query.NewReconcilePartitionGaps(stubHot{}, stubCold{}, nil, metrics)

	srv := NewServer(ServerConfig{
		AuthJWTSecret:      secret,
		RateLimitPerMinute: 4,
		Reconciler:         reconciler,
		Metrics:            metrics,
	})

	body := `{"tenant_id":"tenant-corp-01","partition":3,"from_seq":"100","to_seq":"105"}`

	// 1. Missing token -> 401
	req := httptest.NewRequest(http.MethodPost, "/v1/reconcile/gaps", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// 2. Wrong tenant -> 403
	wrongTenantToken := signTestJWT(t, secret, authClaims{
		Scope:    "audit:reconcile",
		TenantID: "tenant-other",
		Exp:      time.Now().Add(time.Hour).Unix(),
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/reconcile/gaps", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+wrongTenantToken)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for tenant mismatch, got %d", rec.Code)
	}

	// 3. Valid token -> 200 with 1 gap in s3 (seq 105)
	validToken := signTestJWT(t, secret, authClaims{
		Scope:    "audit:reconcile",
		TenantID: "tenant-corp-01",
		Exp:      time.Now().Add(time.Hour).Unix(),
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/reconcile/gaps", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+validToken)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID header")
	}
	if !strings.Contains(rec.Body.String(), `"status":"inconsistent"`) || !strings.Contains(rec.Body.String(), `"missing_in":"s3"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}

	// 4. Exhaust rate limit (capacity 4, 3 used above -> 1 more succeeds, next returns 429)
	req = httptest.NewRequest(http.MethodPost, "/v1/reconcile/gaps", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+validToken)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 4th request to pass, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/reconcile/gaps", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+validToken)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on 5th request, got %d", rec.Code)
	}
}
