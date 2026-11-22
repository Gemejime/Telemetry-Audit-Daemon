package main

import (
	"context"
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetricsWithTenantAllowlist(splitCSV(os.Getenv("METRICS_TENANT_ALLOWLIST")))
	bp := pool.NewBytePool(pool.DefaultInitialCap, pool.DefaultMaxCap)
	redactor := redaction.NewEngine(bp)
	seqTracker := entity.NewSequenceTracker(entity.DefaultMaxTrackedStreams)

	walDir := envOrDefault("WAL_DIR", "/var/lib/audit-daemon/wal")
	walMaxBytes := envInt64OrDefault("WAL_MAX_BYTES", disk.DefaultMaxWALBytes)
	wal, err := disk.NewMmapWAL(walDir, walMaxBytes, disk.DefaultSegmentBytes, logger)
	if err != nil {
		logger.Error("failed to initialize mmap_wal", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = wal.Close() }()

	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Addr:       splitCSV(envOrDefault("CLICKHOUSE_ADDR", "localhost:9000")),
		Database:   envOrDefault("CLICKHOUSE_DATABASE", "default"),
		Username:   envOrDefault("CLICKHOUSE_USER", "default"),
		Password:   os.Getenv("CLICKHOUSE_PASSWORD"),
		TLSEnabled: envBoolOrDefault("CLICKHOUSE_TLS", false),
		Logger:     logger,
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

	wormUploader, err := s3.NewWORMUploader(ctx, s3.Config{
		Bucket:         envOrDefault("S3_BUCKET", "rag-audit-worm-vault"),
		Region:         envOrDefault("S3_REGION", "us-east-1"),
		Endpoint:       os.Getenv("S3_ENDPOINT"),
		KMSKeyID:       os.Getenv("S3_KMS_KEY_ID"),
		UsePathStyle:   envBoolOrDefault("S3_USE_PATH_STYLE", false),
		RetentionYears: envIntOrDefault("S3_RETENTION_YEARS", s3.DefaultRetentionYears),
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
	reconciler := query.NewReconcilePartitionGaps(hotWriter, wormUploader, logger, metrics)

	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Brokers:     splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		Topic:       envOrDefault("KAFKA_TOPIC", kafka.DefaultTopic),
		GroupID:     envOrDefault("KAFKA_GROUP_ID", kafka.DefaultGroupID),
		Processor:   processor,
		HotFlusher:  hotFlusher,
		ColdFlusher: coldFlusher,
		WAL:         wal,
		Logger:      logger,
		Metrics:     metrics,
	})
	if err != nil {
		logger.Error("failed to initialize kafka consumer group", slog.String("error", err.Error()))
		os.Exit(1)
	}

	httpServer := transporthttp.NewServer(transporthttp.ServerConfig{
		Addr:               envOrDefault("HTTP_ADDR", ":8080"),
		MetricsAddr:        os.Getenv("METRICS_ADDR"),
		AuthJWTSecret:      os.Getenv("AUTH_JWT_SECRET"),
		AuthJWTAudience:    envOrDefault("AUTH_JWT_AUDIENCE", "rag-telemetry-audit-daemon"),
		StaticAdminToken:   os.Getenv("STATIC_ADMIN_TOKEN"),
		RateLimitPerMinute: envIntOrDefault("RECONCILE_RATE_LIMIT_PER_MIN", 30),
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

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64OrDefault(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envBoolOrDefault(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
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
