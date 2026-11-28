package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

type mockS3API struct {
	mu          sync.Mutex
	objects     map[string][]byte
	putInputs   []*s3.PutObjectInput
	failUntil   int
	putCalls    int
	rangeReads  int
	statusError int
}

func newMockS3API() *mockS3API {
	return &mockS3API{
		objects:     make(map[string][]byte),
		statusError: http.StatusServiceUnavailable,
	}
}

func (m *mockS3API) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls++
	if m.putCalls <= m.failUntil {
		return nil, &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: m.statusError}},
			Err:      errors.New("SlowDown"),
		}
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	key := aws.ToString(params.Key)
	m.objects[key] = data
	m.putInputs = append(m.putInputs, params)
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3API) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := aws.ToString(params.Prefix)
	var contents []types.Object
	for k, v := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keyCopy := k
			sz := int64(len(v))
			contents = append(contents, types.Object{
				Key:  &keyCopy,
				Size: &sz,
			})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func (m *mockS3API) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[aws.ToString(params.Key)]
	if !ok {
		return nil, errors.New("NoSuchKey")
	}
	rng := aws.ToString(params.Range)
	if rng != "" {
		m.rangeReads++
		spec := strings.TrimPrefix(rng, "bytes=")
		parts := strings.SplitN(spec, "-", 2)
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		data = data[start : end+1]
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func sampleEntries(tenantID, producerID string, partition uint16, minSeq, maxSeq uint64) (entity.BatchManifest, []entity.ComplianceEntry) {
	entries := make([]entity.ComplianceEntry, 0, maxSeq-minSeq+1)
	for seq := minSeq; seq <= maxSeq; seq++ {
		entries = append(entries, entity.ComplianceEntry{
			EventID:               fmt.Sprintf("evt-%d", seq),
			TraceID:               fmt.Sprintf("trc-%d", seq),
			TenantID:              tenantID,
			ProducerID:            producerID,
			KafkaPartition:        partition,
			KafkaOffset:           1000 + seq,
			SeqID:                 seq,
			EventTime:             time.Unix(1700000000+int64(seq), 0).UTC(),
			Plane:                 entity.PlaneWrite,
			Action:                "document.upload",
			ServiceName:           "rag-kb-catalog-service",
			ActorID:               "actor-1",
			AuthMethod:            "oidc",
			SourceIPHash:          strings.Repeat("b", 64),
			DocIDs:                []uint32{10, 20},
			ChunkIDs:              []string{"c-1"},
			DocumentVersions:      []string{"v1"},
			CacheHit:              false,
			EmbeddingModelVersion: "text-embedding-004",
			PayloadRedacted:       []byte(`{"email":"[REDACTED]"}`),
			PromptTokens:          100,
			CompletionTokens:      50,
		})
	}
	manifest := entity.BatchManifest{
		Key: entity.StreamKey{
			ProducerID:     producerID,
			TenantID:       tenantID,
			KafkaPartition: partition,
		},
		MinSeq:      minSeq,
		MaxSeq:      maxSeq,
		RecordCount: len(entries),
	}
	return manifest, entries
}

func TestParquetAndWORMUploader_RoundTripComplianceHeadersAndRangeMetadata(t *testing.T) {
	mock := newMockS3API()
	mock.failUntil = 2
	metrics := observability.NewMetrics()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	uploader, err := NewWORMUploader(context.Background(), Config{
		Bucket:           "compliance-vault",
		KMSKeyID:         "arn:aws:kms:us-east-1:123456789012:key/test-cmk",
		RetentionYears:   7,
		MaxRetries:       3,
		RetryBaseBackoff: time.Millisecond,
		RetryMaxBackoff:  5 * time.Millisecond,
		Client:           mock,
		Logger:           logger,
		Metrics:          metrics,
	})
	if err != nil {
		t.Fatalf("NewWORMUploader: %v", err)
	}

	manifest, entries := sampleEntries("tenant-corp-01", "prod-a", 3, 100, 105)
	if err := uploader.WriteBatch(context.Background(), manifest, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if mock.putCalls != 3 {
		t.Fatalf("expected 3 PutObject attempts (2 503 retries + 1 success), got %d", mock.putCalls)
	}
	if len(mock.putInputs) != 1 {
		t.Fatalf("expected 1 successful PutObjectInput, got %d", len(mock.putInputs))
	}
	in := mock.putInputs[0]
	if in.ObjectLockMode != types.ObjectLockModeCompliance {
		t.Fatalf("expected COMPLIANCE ObjectLockMode, got %s", in.ObjectLockMode)
	}
	if in.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
		t.Fatalf("expected aws:kms encryption, got %s", in.ServerSideEncryption)
	}
	if !aws.ToBool(in.BucketKeyEnabled) {
		t.Fatal("expected BucketKeyEnabled=true")
	}
	if in.ObjectLockRetainUntilDate == nil || time.Until(*in.ObjectLockRetainUntilDate) < 6*365*24*time.Hour {
		t.Fatalf("expected ~7-year ObjectLockRetainUntilDate, got %v", in.ObjectLockRetainUntilDate)
	}
	if !strings.HasPrefix(aws.ToString(in.Key), "tenant_tenant-corp-01/") || !strings.HasSuffix(aws.ToString(in.Key), "/p3_100_105.parquet") {
		t.Fatalf("unexpected S3 key format: %s", aws.ToString(in.Key))
	}

	listed, err := uploader.ListManifests(context.Background(), "tenant-corp-01", 3, 90, 110)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(listed))
	}
	if listed[0].ProducerID != "prod-a" || listed[0].MinSeq != 100 || listed[0].MaxSeq != 105 || listed[0].RecordCount != 6 || listed[0].SchemaVersion != SchemaVersionV1 {
		t.Fatalf("unexpected manifest metadata: %+v", listed[0])
	}
	if mock.rangeReads == 0 {
		t.Fatal("expected HTTP Range reads for Parquet footer")
	}
}
