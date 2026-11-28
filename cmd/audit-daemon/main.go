package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"rag-telemetry-audit-daemon/api/openapi"
	"rag-telemetry-audit-daemon/internal/application/command"
	"rag-telemetry-audit-daemon/internal/application/query"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/infrastructure/config"
	"rag-telemetry-audit-daemon/internal/infrastructure/kafka"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/clickhouse"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/disk"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/s3"
	transporthttp "rag-telemetry-audit-daemon/internal/infrastructure/transport/http"
	"rag-telemetry-audit-daemon/pkg/pool"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

type daemonReadiness struct {
	cg       *kafka.ConsumerGroup
	hotStore *clickhouse.RowBinaryWriter
	wal      *disk.MmapWAL
}

func (r *daemonReadiness) IsDraining() bool                         { return r.cg.IsDraining() }
func (r *daemonReadiness) IsKafkaHealthy() bool                     { return r.cg.IsHealthy() }
func (r *daemonReadiness) PingClickHouse(ctx context.Context) error { return r.hotStore.Ping(ctx) }
func (r *daemonReadiness) IsWALSpilloverActive() bool               { return r.wal.IsSpilloverActive() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := config.LoadDotEnv(); err != nil {
		logger.Error("failed to load .env file", slog.String("error", err.Error()))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetricsWithTenantAllowlist(splitCSV(os.Getenv("METRICS_TENANT_ALLOWLIST")))
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	redactor := redaction.NewEngine(bp)
	seqTracker := entity.NewSequenceTracker(entity.DefaultMaxTrackedStreams)

	walDir := envOrDefault("WAL_DIR", "/var/lib/audit-daemon/wal")
	walMaxBytes, err := envInt64OrDefault("WAL_MAX_BYTES", disk.DefaultMaxWALBytes)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "WAL_MAX_BYTES"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	wal, err := disk.NewMmapWAL(walDir, walMaxBytes, disk.DefaultSegmentBytes, logger)
	if err != nil {
		logger.Error("failed to initialize mmap_wal", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = wal.Close() }()

	chTLS, err := envBoolOrDefault("CLICKHOUSE_TLS", false)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "CLICKHOUSE_TLS"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	chTLSInsecure, err := envBoolOrDefault("CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY", false)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Addr:                  splitCSV(envOrDefault("CLICKHOUSE_ADDR", "localhost:9000")),
		Database:              envOrDefault("CLICKHOUSE_DATABASE", "default"),
		Username:              envOrDefault("CLICKHOUSE_USER", "default"),
		Password:              os.Getenv("CLICKHOUSE_PASSWORD"),
		TLSEnabled:            chTLS,
		TLSInsecureSkipVerify: chTLSInsecure,
		Logger:                logger,
	})
	if err != nil {
		logger.Error("failed to initialize clickhouse client", slog.String("error", err.Error()))
		os.Exit(1)
	}
	hotWriter, err := clickhouse.NewRowBinaryWriter(chClient, clickhouse.RetryConfig{}, logger, metrics)
	if err != nil {
		logger.Error("failed to initialize clickhouse row binary writer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = hotWriter.Close() }()

	s3UsePathStyle, err := envBoolOrDefault("S3_USE_PATH_STYLE", false)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "S3_USE_PATH_STYLE"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	s3RetentionYears, err := envIntOrDefault("S3_RETENTION_YEARS", s3.DefaultRetentionYears)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "S3_RETENTION_YEARS"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	wormUploader, err := s3.NewWORMUploader(ctx, s3.Config{
		Bucket:         envOrDefault("S3_BUCKET", "rag-audit-worm-vault"),
		Region:         envOrDefault("S3_REGION", "us-east-1"),
		Endpoint:       os.Getenv("S3_ENDPOINT"),
		KMSKeyID:       os.Getenv("S3_KMS_KEY_ID"),
		UsePathStyle:   s3UsePathStyle,
		RetentionYears: s3RetentionYears,
		Logger:         logger,
		Metrics:        metrics,
	})
	if err != nil {
		logger.Error("failed to initialize s3 worm uploader", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = wormUploader.Close() }()

	hotFlusher := command.NewHotFlusher(hotWriter, command.DefaultHotFlushRecords, command.DefaultHotFlushInterval)
	coldFlusher := command.NewColdFlusher(wormUploader, command.DefaultColdFlushBytes, command.DefaultColdFlushInterval, command.DefaultColdUploadConcurrency)
	processor := command.NewProcessKafkaBatch(redactor, seqTracker, hotFlusher, coldFlusher, hotWriter, wormUploader, wal, logger, metrics)
	if rawBindings := os.Getenv("PRODUCER_TENANT_BINDINGS"); rawBindings != "" {
		bindings, err := parseProducerTenantBindings(rawBindings)
		if err != nil {
			logger.Error("invalid environment configuration", slog.String("env", "PRODUCER_TENANT_BINDINGS"), slog.String("error", err.Error()))
			os.Exit(1)
		}
		processor.SetProducerTenantBindings(bindings)
	}
	reconciler := query.NewReconcilePartitionGaps(hotWriter, wormUploader, logger, metrics)

	kafkaTLS, err := envBoolOrDefault("KAFKA_TLS", false)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "KAFKA_TLS"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Brokers:       splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		Topic:         envOrDefault("KAFKA_TOPIC", kafka.DefaultTopic),
		GroupID:       envOrDefault("KAFKA_GROUP_ID", kafka.DefaultGroupID),
		TLSEnabled:    kafkaTLS,
		SASLMechanism: envOrDefault("KAFKA_SASL_MECHANISM", "PLAIN"),
		SASLUsername:  os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:  os.Getenv("KAFKA_SASL_PASSWORD"),
		Processor:     processor,
		HotFlusher:    hotFlusher,
		ColdFlusher:   coldFlusher,
		WAL:           wal,
		Logger:        logger,
		Metrics:       metrics,
	})
	if err != nil {
		logger.Error("failed to initialize kafka consumer group", slog.String("error", err.Error()))
		os.Exit(1)
	}

	reconcileRateLimit, err := envIntOrDefault("RECONCILE_RATE_LIMIT_PER_MIN", 30)
	if err != nil {
		logger.Error("invalid environment configuration", slog.String("env", "RECONCILE_RATE_LIMIT_PER_MIN"), slog.String("error", err.Error()))
		os.Exit(1)
	}
	httpServer := transporthttp.NewServer(transporthttp.ServerConfig{
		Addr:               envOrDefault("HTTP_ADDR", ":8080"),
		MetricsAddr:        os.Getenv("METRICS_ADDR"),
		AuthJWTSecret:      os.Getenv("AUTH_JWT_SECRET"),
		AuthJWTAudience:    envOrDefault("AUTH_JWT_AUDIENCE", "rag-telemetry-audit-daemon"),
		StaticAdminToken:   os.Getenv("STATIC_ADMIN_TOKEN"),
		RateLimitPerMinute: reconcileRateLimit,
		OpenAPISpecYAML:    openapi.SpecYAML,
		HealthChecker:      &daemonReadiness{cg: cg, hotStore: hotWriter, wal: wal},
		Reconciler:         reconciler,
		Metrics:            metrics,
		Logger:             logger,
	})
	httpServer.Start()

	logger.Info("rag-telemetry-audit-daemon started",
		slog.String("http_addr", envOrDefault("HTTP_ADDR", ":8080")),
		slog.String("topic", envOrDefault("KAFKA_TOPIC", kafka.DefaultTopic)),
	)

	if err := cg.Run(ctx); err != nil {
		logger.Error("kafka consumer group exited with error", slog.String("error", err.Error()))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http server shutdown error", slog.String("error", err.Error()))
	}
	logger.Info("rag-telemetry-audit-daemon stopped")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) (int, error) {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", key, err)
		}
		return n, nil
	}
	return def, nil
}

func envInt64OrDefault(key string, def int64) (int64, error) {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", key, err)
		}
		return n, nil
	}
	return def, nil
}

func envBoolOrDefault(key string, def bool) (bool, error) {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("parse %s: %w", key, err)
		}
		return b, nil
	}
	return def, nil
}

func parseProducerTenantBindings(raw string) (map[string]string, error) {
	bindings := make(map[string]string)
	for _, pair := range splitCSV(raw) {
		producerID, tenantID, ok := strings.Cut(pair, "=")
		producerID = strings.TrimSpace(producerID)
		tenantID = strings.TrimSpace(tenantID)
		if !ok || producerID == "" || tenantID == "" {
			return nil, fmt.Errorf("expected producer_id=tenant_id entry, got %q", pair)
		}
		bindings[producerID] = tenantID
	}
	return bindings, nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
