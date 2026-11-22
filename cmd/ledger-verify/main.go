package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"rag-telemetry-audit-daemon/internal/application/query"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/clickhouse"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/s3"
)

type cliOptions struct {
	tenantID       string
	partition      uint
	fromSeq        uint64
	toSeq          uint64
	s3Bucket       string
	s3Region       string
	s3Endpoint     string
	s3PathStyle    bool
	chAddr         string
	chDatabase     string
	chUser         string
	chPassword     string
	timeoutSeconds int
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	fs := flag.NewFlagSet("ledger-verify", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var opts cliOptions
	fs.StringVar(&opts.tenantID, "tenant-id", "", "Tenant ID to verify (required)")
	fs.UintVar(&opts.partition, "partition", 0, "Kafka partition to verify")
	fs.Uint64Var(&opts.fromSeq, "from-seq", 1, "Inclusive lower sequence ID")
	fs.Uint64Var(&opts.toSeq, "to-seq", 0, "Inclusive upper sequence ID (required)")
	fs.StringVar(&opts.s3Bucket, "s3-bucket", os.Getenv("S3_BUCKET"), "S3 WORM vault bucket name")
	fs.StringVar(&opts.s3Region, "s3-region", envOr("S3_REGION", "us-east-1"), "AWS S3 region")
	fs.StringVar(&opts.s3Endpoint, "s3-endpoint", os.Getenv("S3_ENDPOINT"), "Optional S3 endpoint override")
	fs.BoolVar(&opts.s3PathStyle, "s3-path-style", false, "Use S3 path-style addressing")
	fs.StringVar(&opts.chAddr, "clickhouse-addr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native TCP address")
	fs.StringVar(&opts.chDatabase, "clickhouse-db", envOr("CLICKHOUSE_DATABASE", "default"), "ClickHouse database")
	fs.StringVar(&opts.chUser, "clickhouse-user", envOr("CLICKHOUSE_USER", "default"), "ClickHouse username")
	fs.StringVar(&opts.chPassword, "clickhouse-password", os.Getenv("CLICKHOUSE_PASSWORD"), "ClickHouse password")
	fs.IntVar(&opts.timeoutSeconds, "timeout", 60, "Verification timeout in seconds")

	if err := fs.Parse(args); err != nil {
		return 1
	}
	if opts.tenantID == "" || opts.toSeq < opts.fromSeq || opts.s3Bucket == "" || opts.partition > 65535 {
		logger.Error("invalid arguments: --tenant-id, --s3-bucket, and --to-seq >= --from-seq are required")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.timeoutSeconds)*time.Second)
	defer cancel()

	metrics := observability.NewMetrics()

	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Addr:     strings.Split(opts.chAddr, ","),
		Database: opts.chDatabase,
		Username: opts.chUser,
		Password: opts.chPassword,
		Logger:   logger,
	})
	if err != nil {
		logger.Error("failed to connect to clickhouse", slog.String("error", err.Error()))
		return 1
	}
	hotWriter, err := clickhouse.NewRowBinaryWriter(chClient, clickhouse.RetryConfig{}, logger, metrics)
	if err != nil {
		logger.Error("failed to initialize clickhouse writer", slog.String("error", err.Error()))
		return 1
	}
	defer func() { _ = hotWriter.Close() }()

	wormUploader, err := s3.NewWORMUploader(ctx, s3.Config{
		Bucket:       opts.s3Bucket,
		Region:       opts.s3Region,
		Endpoint:     opts.s3Endpoint,
		UsePathStyle: opts.s3PathStyle,
		Logger:       logger,
		Metrics:      metrics,
	})
	if err != nil {
		logger.Error("failed to initialize s3 worm uploader", slog.String("error", err.Error()))
		return 1
	}
	defer func() { _ = wormUploader.Close() }()

	return executeVerify(ctx, opts, hotWriter, wormUploader, logger, metrics, stdout)
}

func executeVerify(
	ctx context.Context,
	opts cliOptions,
	hotStore repository.HotStorePort,
	coldStore repository.ColdStorePort,
	logger *slog.Logger,
	metrics *observability.Metrics,
	stdout io.Writer,
) int {
	reconciler := query.NewReconcilePartitionGaps(hotStore, coldStore, logger, metrics)
	res, err := reconciler.Execute(ctx, query.ReconcileRequest{
		TenantID:  opts.tenantID,
		Partition: uint16(opts.partition), // #nosec G115 -- validated <= 65535
		FromSeq:   opts.fromSeq,
		ToSeq:     opts.toSeq,
	})
	if err != nil {
		logger.Error("reconciliation failed", slog.String("error", err.Error()))
		return 1
	}

	gapsOut := make([]map[string]string, 0, len(res.Gaps))
	for _, g := range res.Gaps {
		gapsOut = append(gapsOut, map[string]string{
			"producer_id": g.ProducerID,
			"min_seq":     strconv.FormatUint(g.MinSeq, 10),
			"max_seq":     strconv.FormatUint(g.MaxSeq, 10),
			"missing_in":  g.MissingIn,
		})
	}

	overlapsOut := make([]map[string]string, 0, len(res.Overlaps))
	for _, ov := range res.Overlaps {
		overlapsOut = append(overlapsOut, map[string]string{
			"producer_id": ov.ProducerID,
			"key_a":       ov.KeyA,
			"key_b":       ov.KeyB,
			"min_seq":     strconv.FormatUint(ov.MinSeq, 10),
			"max_seq":     strconv.FormatUint(ov.MaxSeq, 10),
		})
	}

	report := map[string]any{
		"tenant_id": res.TenantID,
		"partition": res.Partition,
		"checked_range": map[string]string{
			"min_seq": strconv.FormatUint(res.CheckedRange.MinSeq, 10),
			"max_seq": strconv.FormatUint(res.CheckedRange.MaxSeq, 10),
		},
		"gaps":     gapsOut,
		"overlaps": overlapsOut,
		"status":   res.Status,
	}

	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		logger.Error("failed to encode report", slog.String("error", err.Error()))
		return 1
	}

	if res.Status != "consistent" {
		return 2
	}
	return 0
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
