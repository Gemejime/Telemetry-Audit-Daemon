package http

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"rag-telemetry-audit-daemon/internal/application/query"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

const requiredReconcileScope = "audit:reconcile"

// ServerConfig configures the HTTP admin, probe, reconciliation, and OpenAPI server.
type ServerConfig struct {
	Addr               string
	MetricsAddr        string
	AuthJWTSecret      string
	StaticAdminToken   string
	RateLimitPerMinute int
	OpenAPISpecYAML    []byte
	HealthChecker      ReadinessChecker
	Reconciler         *query.ReconcilePartitionGaps
	Metrics            *observability.Metrics
	Logger             *slog.Logger
}

// Server serves /healthz, /ready, /metrics, /v1/reconcile/gaps, /openapi.yaml, and /docs.
type Server struct {
	mainServer    *http.Server
	metricsServer *http.Server
	cfg           ServerConfig
	logger        *slog.Logger
	limiter       *tokenBucketLimiter
}

// NewServer constructs the HTTP Server with secured logging, bearer auth, and rate limiting.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.RateLimitPerMinute <= 0 {
		cfg.RateLimitPerMinute = 30
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	s := &Server{
		cfg:     cfg,
		logger:  cfg.Logger,
		limiter: newTokenBucketLimiter(cfg.RateLimitPerMinute, time.Minute),
	}

	health := NewHealthHandler(cfg.HealthChecker)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health.Healthz)
	mux.HandleFunc("GET /ready", health.Ready)
	if cfg.Metrics != nil {
		mux.Handle("GET /metrics", cfg.Metrics.Handler())
	}
	mux.HandleFunc("POST /v1/reconcile/gaps", s.handleReconcileGaps)
	mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPISpec)
	mux.HandleFunc("GET /docs", s.handleDocs)

	s.mainServer = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.withMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if cfg.MetricsAddr != "" && cfg.Metrics != nil {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", cfg.Metrics.Handler())
		s.metricsServer = &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      15 * time.Second,
		}
	}

	return s
}

// Handler returns the main HTTP handler (useful for httptest in unit tests).
func (s *Server) Handler() http.Handler {
	return s.mainServer.Handler
}

// Start launches the HTTP server(s) in background goroutines.
func (s *Server) Start() {
	go func() {
		if err := s.mainServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http admin server exited", slog.String("error", err.Error()))
		}
	}()
	if s.metricsServer != nil {
		go func() {
			if err := s.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("http metrics server exited", slog.String("error", err.Error()))
			}
		}()
	}
}

// Shutdown gracefully stops the HTTP server(s).
func (s *Server) Shutdown(ctx context.Context) error {
	var errs []error
	if err := s.mainServer.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type reconcileHTTPRequest struct {
	TenantID  string `json:"tenant_id"`
	Partition int32  `json:"partition"`
	FromSeq   string `json:"from_seq"`
	ToSeq     string `json:"to_seq"`
}

func (s *Server) handleReconcileGaps(w http.ResponseWriter, r *http.Request) {
	reqID := w.Header().Get("X-Request-ID")

	if !s.limiter.allow() {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error":   "RATE_LIMITED",
			"message": "Reconciliation rate limit exceeded",
		})
		return
	}

	claims, status, errMsg := s.authenticateRequest(r.Header.Get("Authorization"))
	if status != 0 {
		code := "UNAUTHORIZED"
		if status == http.StatusForbidden {
			code = "FORBIDDEN"
		}
		writeJSON(w, status, map[string]string{
			"error":   code,
			"message": errMsg,
		})
		return
	}

	var body reconcileHTTPRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 64*1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Invalid JSON request body",
		})
		return
	}

	if body.TenantID == "" || body.Partition < 0 || body.Partition > 65535 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Invalid tenant_id or partition",
		})
		return
	}

	fromSeq, err := strconv.ParseUint(body.FromSeq, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Invalid from_seq uint64 decimal string",
		})
		return
	}
	toSeq, err := strconv.ParseUint(body.ToSeq, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Invalid to_seq uint64 decimal string",
		})
		return
	}
	if fromSeq > toSeq {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "BAD_REQUEST",
			"message": "Invalid tenant_id, negative partition, or from_seq > to_seq",
		})
		return
	}

	if !claims.allowsTenant(body.TenantID) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error":   "FORBIDDEN",
			"message": "Token is not authorized for the requested tenant_id",
		})
		return
	}

	if s.cfg.Reconciler == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":   "STORAGE_UNAVAILABLE",
			"message": "Reconciliation service is not configured",
		})
		return
	}

	start := time.Now()
	res, err := s.cfg.Reconciler.Execute(r.Context(), query.ReconcileRequest{
		TenantID:  body.TenantID,
		Partition: uint16(body.Partition),
		FromSeq:   fromSeq,
		ToSeq:     toSeq,
	})
	if err != nil {
		s.logger.Error("reconcile_partition_gaps failed",
			slog.String("trace_id", reqID),
			slog.String("tenant_id", body.TenantID),
			slog.Int("partition", int(body.Partition)),
			slog.String("error", err.Error()),
		)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":   "STORAGE_UNAVAILABLE",
			"message": "ClickHouse or S3 metadata store temporarily unavailable",
		})
		return
	}

	gapsJSON := make([]map[string]string, 0, len(res.Gaps))
	for _, g := range res.Gaps {
		gapsJSON = append(gapsJSON, map[string]string{
			"producer_id": g.ProducerID,
			"min_seq":     strconv.FormatUint(g.MinSeq, 10),
			"max_seq":     strconv.FormatUint(g.MaxSeq, 10),
			"missing_in":  g.MissingIn,
		})
	}

	s.logger.Info("reconcile_partition_gaps completed",
		slog.String("trace_id", reqID),
		slog.String("tenant_id", body.TenantID),
		slog.Int("partition", int(body.Partition)),
		slog.Uint64("min_seq", fromSeq),
		slog.Uint64("max_seq", toSeq),
		slog.Int("gap_count", len(gapsJSON)),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": res.TenantID,
		"partition": res.Partition,
		"checked_range": map[string]string{
			"min_seq": strconv.FormatUint(res.CheckedRange.MinSeq, 10),
			"max_seq": strconv.FormatUint(res.CheckedRange.MaxSeq, 10),
		},
		"gaps":   gapsJSON,
		"status": res.Status,
	})
}

func (s *Server) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	if len(s.cfg.OpenAPISpecYAML) == 0 {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.cfg.OpenAPISpecYAML)
}

func (s *Server) handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>RAG Telemetry &amp; Audit Daemon API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui' });
    };
  </script>
</body>
</html>`))
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = randomRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)

		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("http handler panic recovered",
					slog.String("trace_id", reqID),
					slog.String("path", r.URL.Path),
				)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error":   "INTERNAL_ERROR",
					"message": "Internal server error",
				})
			}
		}()

		next.ServeHTTP(w, r)
	})
}

type authClaims struct {
	Scope    string   `json:"scope"`
	TenantID string   `json:"tenant_id"`
	Tenants  []string `json:"tenants"`
	Exp      int64    `json:"exp"`
}

func (c authClaims) hasScope(target string) bool {
	for _, s := range strings.Fields(c.Scope) {
		if s == target || s == "*" {
			return true
		}
	}
	return false
}

func (c authClaims) allowsTenant(tenantID string) bool {
	if c.TenantID == "*" {
		return true
	}
	if c.TenantID != "" && c.TenantID == tenantID {
		return true
	}
	for _, t := range c.Tenants {
		if t == "*" || t == tenantID {
			return true
		}
	}
	return false
}

func (s *Server) authenticateRequest(authHeader string) (authClaims, int, string) {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return authClaims{}, http.StatusUnauthorized, "Missing or invalid bearer token"
	}
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if token == "" {
		return authClaims{}, http.StatusUnauthorized, "Missing or invalid bearer token"
	}

	if s.cfg.StaticAdminToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.StaticAdminToken)) == 1 {
		return authClaims{Scope: requiredReconcileScope, TenantID: "*"}, 0, ""
	}

	if s.cfg.AuthJWTSecret == "" {
		return authClaims{}, http.StatusUnauthorized, "Authentication not configured or invalid token"
	}

	claims, err := verifyHS256JWT(token, []byte(s.cfg.AuthJWTSecret))
	if err != nil {
		return authClaims{}, http.StatusUnauthorized, "Invalid or expired bearer token"
	}
	if !claims.hasScope(requiredReconcileScope) {
		return authClaims{}, http.StatusForbidden, "Missing or insufficient audit:reconcile scope"
	}
	return claims, 0, ""
}

func verifyHS256JWT(token string, secret []byte) (authClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return authClaims{}, errors.New("malformed jwt")
	}

	hdrBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return authClaims{}, err
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(hdrBytes, &hdr); err != nil || hdr.Alg != "HS256" {
		return authClaims{}, errors.New("unsupported jwt alg")
	}

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSig := mac.Sum(nil)

	gotSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return authClaims{}, err
	}
	if !hmac.Equal(gotSig, expectedSig) {
		return authClaims{}, errors.New("invalid jwt signature")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return authClaims{}, err
	}
	var claims authClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return authClaims{}, err
	}
	if claims.Exp > 0 && time.Now().UTC().Unix() > claims.Exp {
		return authClaims{}, errors.New("jwt expired")
	}
	return claims, nil
}

type tokenBucketLimiter struct {
	mu       sync.Mutex
	capacity int
	tokens   int
	interval time.Duration
	lastFill time.Time
}

func newTokenBucketLimiter(capacity int, interval time.Duration) *tokenBucketLimiter {
	return &tokenBucketLimiter{
		capacity: capacity,
		tokens:   capacity,
		interval: interval,
		lastFill: time.Now(),
	}
}

func (l *tokenBucketLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if now.Sub(l.lastFill) >= l.interval {
		l.tokens = l.capacity
		l.lastFill = now
	}
	if l.tokens <= 0 {
		return false
	}
	l.tokens--
	return true
}

func randomRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
