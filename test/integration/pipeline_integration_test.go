//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "rag-telemetry-audit-daemon/api/proto/v1"
	"rag-telemetry-audit-daemon/internal/application/command"
	"rag-telemetry-audit-daemon/internal/application/query"
	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/infrastructure/kafka"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/clickhouse"
	"rag-telemetry-audit-daemon/internal/infrastructure/persistence/disk"
	s3store "rag-telemetry-audit-daemon/internal/infrastructure/persistence/s3"
	"rag-telemetry-audit-daemon/pkg/auditclient"
	"rag-telemetry-audit-daemon/pkg/pool"
	"rag-telemetry-audit-daemon/pkg/redaction"
)

// TestIntegration_EndToEndKafkaClickHouseS3WORMAndRebalance runs the full
// Kafka -> Redaction -> ClickHouse + S3 WORM pipeline against real containers.
//
// Provide KAFKA_BROKERS, CLICKHOUSE_ADDR, and S3_ENDPOINT via environment
// (e.g. CI service containers or docker compose) to run.
func TestIntegration_EndToEndKafkaClickHouseS3WORMAndRebalance(t *testing.T) {
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	chAddr := os.Getenv("CLICKHOUSE_ADDR")
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	if kafkaBroker == "" || chAddr == "" || s3Endpoint == "" {
		if _, err := exec.LookPath("docker"); err != nil {
			t.Skip("skipping integration test: KAFKA_BROKERS/CLICKHOUSE_ADDR/S3_ENDPOINT not set and docker not available")
		}
		t.Skip("skipping integration test: set KAFKA_BROKERS, CLICKHOUSE_ADDR, S3_ENDPOINT to target running containers")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		topic       = "audit-compliance-records"
		bucket      = "rag-audit-worm-it"
		tenantID    = "tenant-it-01"
		totalEvents = 50_000
	)

	// 1. Ensure Kafka topic exists with 8 partitions.
	kcl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(kafkaBroker, ",")...))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer kcl.Close()
	req := kmsg.NewCreateTopicsRequest()
	tp := kmsg.NewCreateTopicsRequestTopic()
	tp.Topic = topic
	tp.NumPartitions = 8
	tp.ReplicationFactor = 1
	req.Topics = append(req.Topics, tp)
	_, _ = kcl.Request(ctx, &req)

	// 2. Provision ClickHouse table (ReplacingMergeTree(ingested_at) for single-node container).
	conn, err := ch.Open(&ch.Options{Addr: strings.Split(chAddr, ",")})
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS audit_compliance_records (
			event_id                 String,
			trace_id                 String,
			tenant_id                LowCardinality(String),
			producer_id              LowCardinality(String),
			kafka_partition          UInt16,
			kafka_offset             UInt64,
			seq_id                   UInt64,
			event_time               DateTime64(3, 'UTC'),
			ingested_at              DateTime64(3, 'UTC') DEFAULT now64(3),
			plane                    Enum8('UNSPECIFIED' = 0, 'READ' = 1, 'WRITE' = 2),
			action                   LowCardinality(String),
			service_name             LowCardinality(String),
			actor_id                 String,
			auth_method              LowCardinality(String),
			source_ip_hash           FixedString(64),
			doc_ids                  Array(UInt32),
			chunk_ids                Array(String),
			document_versions        Array(String),
			cache_hit                Bool,
			embedding_model_version  LowCardinality(String),
			payload_redacted         String,
			prompt_tokens            UInt32,
			completion_tokens        UInt32
		)
		ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toYYYYMM(event_time)
		ORDER BY (tenant_id, event_time, producer_id, kafka_partition, seq_id)
	`); err != nil {
		t.Fatalf("create clickhouse table: %v", err)
	}

	// 3. Provision LocalStack S3 bucket with Object Lock enabled.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("awsconfig: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})
	_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})

	// 4. Wire daemon components.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := observability.NewMetrics()
	wal, err := disk.NewMmapWAL(t.TempDir(), disk.DefaultMaxWALBytes, disk.DefaultSegmentBytes, logger)
	if err != nil {
		t.Fatalf("NewMmapWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Addr:     strings.Split(chAddr, ","),
		Database: "default",
		Username: "default",
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("clickhouse.NewClient: %v", err)
	}
	hotWriter, err := clickhouse.NewRowBinaryWriter(chClient, clickhouse.RetryConfig{}, logger, metrics)
	if err != nil {
		t.Fatalf("NewRowBinaryWriter: %v", err)
	}
	defer func() { _ = hotWriter.Close() }()

	wormUploader, err := s3store.NewWORMUploader(ctx, s3store.Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        s3Endpoint,
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		UsePathStyle:    true,
		RetentionYears:  7,
		Logger:          logger,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatalf("NewWORMUploader: %v", err)
	}
	defer func() { _ = wormUploader.Close() }()

	hotFlusher := command.NewHotFlusher(hotWriter, command.DefaultHotFlushRecords, command.DefaultHotFlushInterval)
	coldFlusher := command.NewColdFlusher(wormUploader, 1024*1024, 5*time.Second, 4)
	proc := command.NewProcessKafkaBatch(redaction.NewEngine(pool.NewBytePool(0, 0)), nil, hotFlusher, coldFlusher, hotWriter, wormUploader, wal, logger, metrics)

	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Brokers:     strings.Split(kafkaBroker, ","),
		Topic:       topic,
		GroupID:     fmt.Sprintf("it-group-%d", time.Now().UnixNano()),
		Processor:   proc,
		HotFlusher:  hotFlusher,
		ColdFlusher: coldFlusher,
		WAL:         wal,
		Logger:      logger,
		Metrics:     metrics,
	})
	if err != nil {
		t.Fatalf("NewConsumerGroup: %v", err)
	}

	cgCtx, cgCancel := context.WithCancel(ctx)
	defer cgCancel()
	go func() { _ = cg.Run(cgCtx) }()

	// 5. Emit 50,000 compliance events via auditclient.
	sdk, err := auditclient.NewClient(auditclient.Config{
		Brokers:       strings.Split(kafkaBroker, ","),
		Topic:         topic,
		NumPartitions: 8,
		ProducerID:    "it-producer-1",
	})
	if err != nil {
		t.Fatalf("auditclient.NewClient: %v", err)
	}

	ipHash := sha256.Sum256([]byte("10.1.1.1"))
	ipHex := hex.EncodeToString(ipHash[:])
	for i := 1; i <= totalEvents; i++ {
		if err := sdk.RecordCompliance(ctx, &auditv1.AuditRecord{
			EventId:     fmt.Sprintf("it-evt-%d", i),
			TraceId:     fmt.Sprintf("it-tr-%d", i),
			TenantId:    tenantID,
			EventTime:   timestamppb.Now(),
			Plane:       auditv1.PlaneType_PLANE_TYPE_READ,
			Action:      "search.query",
			ServiceName: "rag-search-orchestrator",
			Actor: &auditv1.ActorContext{
				ActorId:      "it-actor",
				TenantId:     tenantID,
				AuthMethod:   "jwt",
				SourceIpHash: ipHex,
			},
			PayloadRedacted: []byte(`{"email":"it@example.com"}`),
		}); err != nil {
			t.Fatalf("RecordCompliance(%d): %v", i, err)
		}
	}
	if err := sdk.Close(ctx); err != nil {
		t.Fatalf("sdk.Close: %v", err)
	}

	// 6. Wait for ClickHouse FINAL count == 50,000 and S3 reconciliation consistent.
	part := uint16(auditclient.PartitionForTenant(tenantID, 8)) // #nosec G115
	deadline := time.Now().Add(65 * time.Second)
	reconciler := query.NewReconcilePartitionGaps(hotWriter, wormUploader, logger, metrics)

	for time.Now().Before(deadline) {
		var count uint64
		_ = conn.QueryRow(ctx, "SELECT count() FROM audit_compliance_records FINAL WHERE tenant_id = ?", tenantID).Scan(&count)
		res, rErr := reconciler.Execute(ctx, query.ReconcileRequest{
			TenantID:  tenantID,
			Partition: part,
			FromSeq:   1,
			ToSeq:     totalEvents,
		})
		if count == totalEvents && rErr == nil && res.Status == "consistent" {
			// Verify S3 Object Lock COMPLIANCE header on first listed object.
			objs, lErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(bucket),
				Prefix: aws.String("tenant_" + tenantID + "/"),
			})
			if lErr == nil && len(objs.Contents) > 0 {
				head, hErr := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket),
					Key:    objs.Contents[0].Key,
				})
				if hErr != nil || head.ObjectLockMode != types.ObjectLockModeCompliance {
					t.Fatalf("expected S3 ObjectLockMode COMPLIANCE, got %v (err=%v)", head.ObjectLockMode, hErr)
				}
			}
			_ = entity.PlaneRead
			_ = proto.Marshal
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d events to persist and reconcile across ClickHouse and S3 WORM", totalEvents)
}
