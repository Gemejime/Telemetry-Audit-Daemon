package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"rag-telemetry-audit-daemon/internal/domain/entity"
	"rag-telemetry-audit-daemon/internal/domain/repository"
	"rag-telemetry-audit-daemon/internal/infrastructure/observability"
)

const (
	DefaultRetentionYears                = 7
	DefaultMaxRetries                    = 4
	DefaultRetryBaseBackoff              = 150 * time.Millisecond
	DefaultRetryMaxBackoff               = 5 * time.Second
	DefaultManifestReadConcurrency       = 8
	defaultFooterTailBytes         int64 = 64 * 1024
)

// API abstracts the AWS S3 v2 client operations required for WORM uploads and metadata listing.
type API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Config configures the S3 WORM uploader and Parquet metadata lister.
type Config struct {
	Bucket                  string
	Region                  string
	Endpoint                string
	KMSKeyID                string
	AccessKeyID             string
	SecretAccessKey         string // #nosec G117 -- runtime credential injected via env/secret, never logged
	UsePathStyle            bool
	RetentionYears          int
	MaxRetries              int
	RetryBaseBackoff        time.Duration
	RetryMaxBackoff         time.Duration
	ManifestReadConcurrency int
	Client                  API
	Logger                  *slog.Logger
	Metrics                 *observability.Metrics
}

// WORMUploader implements repository.ColdStorePort over AWS S3 Object Lock (Compliance Mode).
type WORMUploader struct {
	client                  API
	bucket                  string
	kmsKeyID                string
	retentionYears          int
	maxRetries              int
	retryBaseBackoff        time.Duration
	retryMaxBackoff         time.Duration
	manifestReadConcurrency int
	logger                  *slog.Logger
	metrics                 *observability.Metrics
}

// NewWORMUploader initializes an S3 WORMUploader with HTTP/2 connection reuse.
func NewWORMUploader(ctx context.Context, cfg Config) (*WORMUploader, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 bucket is required")
	}
	if cfg.RetentionYears <= 0 {
		cfg.RetentionYears = DefaultRetentionYears
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.RetryBaseBackoff <= 0 {
		cfg.RetryBaseBackoff = DefaultRetryBaseBackoff
	}
	if cfg.RetryMaxBackoff <= 0 {
		cfg.RetryMaxBackoff = DefaultRetryMaxBackoff
	}
	if cfg.ManifestReadConcurrency <= 0 {
		cfg.ManifestReadConcurrency = DefaultManifestReadConcurrency
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	client := cfg.Client
	if client == nil {
		var err error
		client, err = newAWSS3Client(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	return &WORMUploader{
		client:                  client,
		bucket:                  cfg.Bucket,
		kmsKeyID:                cfg.KMSKeyID,
		retentionYears:          cfg.RetentionYears,
		maxRetries:              cfg.MaxRetries,
		retryBaseBackoff:        cfg.RetryBaseBackoff,
		retryMaxBackoff:         cfg.RetryMaxBackoff,
		manifestReadConcurrency: cfg.ManifestReadConcurrency,
		logger:                  cfg.Logger,
		metrics:                 cfg.Metrics,
	}, nil
}

func newAWSS3Client(ctx context.Context, cfg Config) (*s3.Client, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}

	var opts []func(*config.LoadOptions) error
	opts = append(opts,
		config.WithRegion(region),
		config.WithHTTPClient(httpClient),
	)
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	}), nil
}

// BuildObjectKey generates a randomized-prefix S3 key isolating tenants and preventing 503 SlowDown.
func BuildObjectKey(tenantID string, partition uint16, minSeq, maxSeq uint64, ts time.Time) (string, error) {
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", fmt.Errorf("generate s3 key prefix: %w", err)
	}
	hashPrefix := hex.EncodeToString(rnd[:])
	utc := ts.UTC()
	return fmt.Sprintf(
		"tenant_%s/%s/year=%04d/month=%02d/day=%02d/p%d_%d_%d.parquet",
		tenantID,
		hashPrefix,
		utc.Year(),
		int(utc.Month()),
		utc.Day(),
		partition,
		minSeq,
		maxSeq,
	), nil
}

// WriteBatch encodes entries as columnar Parquet and uploads to S3 with Object Lock Compliance Mode.
func (u *WORMUploader) WriteBatch(ctx context.Context, manifest entity.BatchManifest, entries []entity.ComplianceEntry) error {
	if len(entries) == 0 {
		return nil
	}

	start := time.Now()
	payload, err := EncodeParquetBatch(manifest, entries)
	if err != nil {
		return err
	}

	key, err := BuildObjectKey(manifest.Key.TenantID, manifest.Key.KafkaPartition, manifest.MinSeq, manifest.MaxSeq, entries[0].EventTime)
	if err != nil {
		return err
	}

	retainUntil := time.Now().UTC().Add(time.Duration(u.retentionYears) * 365 * 24 * time.Hour)

	err = u.withRetry(ctx, manifest, key, func() error {
		input := &s3.PutObjectInput{
			Bucket:                    aws.String(u.bucket),
			Key:                       aws.String(key),
			Body:                      bytes.NewReader(payload),
			ContentLength:             aws.Int64(int64(len(payload))),
			ContentType:               aws.String("application/vnd.apache.parquet"),
			ObjectLockMode:            types.ObjectLockModeCompliance,
			ObjectLockRetainUntilDate: aws.Time(retainUntil),
			ServerSideEncryption:      types.ServerSideEncryptionAwsKms,
			BucketKeyEnabled:          aws.Bool(true),
			ChecksumAlgorithm:         types.ChecksumAlgorithmSha256,
		}
		if u.kmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(u.kmsKeyID)
		}
		_, putErr := u.client.PutObject(ctx, input)
		return putErr
	})

	elapsed := time.Since(start)
	u.metrics.ObserveColdFlushDuration(elapsed)
	if err != nil {
		return err
	}

	u.logger.Info("s3 worm parquet batch persisted",
		slog.String("trace_id", entries[0].TraceID),
		slog.String("tenant_id", manifest.Key.TenantID),
		slog.String("producer_id", manifest.Key.ProducerID),
		slog.Uint64("partition", uint64(manifest.Key.KafkaPartition)),
		slog.Uint64("min_seq", manifest.MinSeq),
		slog.Uint64("max_seq", manifest.MaxSeq),
		slog.Int("record_count", len(entries)),
		slog.Int("bytes", len(payload)),
		slog.String("s3_key", key),
		slog.Int64("duration_ms", elapsed.Milliseconds()),
	)
	return nil
}

// ListManifests lists S3 Parquet objects for tenantID/partition overlapping [fromSeq, toSeq]
// and reads only their Parquet footers via HTTP Range requests.
func (u *WORMUploader) ListManifests(
	ctx context.Context,
	tenantID string,
	partition uint16,
	fromSeq, toSeq uint64,
) ([]repository.ColdObjectManifest, error) {
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	if fromSeq > toSeq {
		return nil, errors.New("from_seq must be <= to_seq")
	}

	prefix := fmt.Sprintf("tenant_%s/", tenantID)
	type candidate struct {
		key  string
		size int64
	}
	var candidates []candidate

	var token *string
	for {
		out, err := u.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(u.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3 objects for tenant %s: %w", tenantID, err)
		}

		for _, obj := range out.Contents {
			if obj.Key == nil || obj.Size == nil || *obj.Size <= 0 {
				continue
			}
			key := *obj.Key
			if part, objMin, objMax, ok := parseObjectKeyBounds(key); ok {
				if part != partition || objMax < fromSeq || objMin > toSeq {
					continue
				}
			}
			candidates = append(candidates, candidate{key: key, size: *obj.Size})
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	manifests := make([]repository.ColdObjectManifest, 0, len(candidates))
	var mu sync.Mutex
	var firstErr error

	sem := make(chan struct{}, u.manifestReadConcurrency)
	var wg sync.WaitGroup

	for _, c := range candidates {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(cand candidate) {
			defer wg.Done()
			defer func() { <-sem }()

			ra := &s3RangeReaderAt{
				ctx:    ctx,
				client: u.client,
				bucket: u.bucket,
				key:    cand.key,
				size:   cand.size,
			}
			m, err := ReadParquetManifest(ra, cand.size)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("read parquet manifest %s: %w", cand.key, err)
				}
				mu.Unlock()
				return
			}
			m.Key = cand.key
			if m.TenantID == tenantID && m.KafkaPartition == partition && m.MaxSeq >= fromSeq && m.MinSeq <= toSeq {
				mu.Lock()
				manifests = append(manifests, m)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].ProducerID != manifests[j].ProducerID {
			return manifests[i].ProducerID < manifests[j].ProducerID
		}
		if manifests[i].MinSeq != manifests[j].MinSeq {
			return manifests[i].MinSeq < manifests[j].MinSeq
		}
		return manifests[i].MaxSeq < manifests[j].MaxSeq
	})

	return manifests, nil
}

// Close satisfies repository.ColdStorePort.
func (u *WORMUploader) Close() error {
	return nil
}

func (u *WORMUploader) withRetry(ctx context.Context, manifest entity.BatchManifest, key string, fn func() error) error {
	backoff := u.retryBaseBackoff
	var lastErr error

	for attempt := 0; attempt <= u.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
			u.metrics.IncS3UploadError(classifyS3ErrorStatus(err))
		}

		if attempt == u.maxRetries {
			break
		}

		u.logger.Warn("s3 worm upload failed, retrying",
			slog.String("tenant_id", manifest.Key.TenantID),
			slog.String("producer_id", manifest.Key.ProducerID),
			slog.Uint64("partition", uint64(manifest.Key.KafkaPartition)),
			slog.Uint64("min_seq", manifest.MinSeq),
			slog.Uint64("max_seq", manifest.MaxSeq),
			slog.String("s3_key", key),
			slog.Int("attempt", attempt+1),
			slog.Int64("backoff_ms", backoff.Milliseconds()),
			slog.String("error", lastErr.Error()),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > u.retryMaxBackoff {
			backoff = u.retryMaxBackoff
		}
	}

	return lastErr
}

func classifyS3ErrorStatus(err error) string {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return strconv.Itoa(respErr.HTTPStatusCode())
	}
	msg := err.Error()
	if strings.Contains(msg, "503") || strings.Contains(msg, "SlowDown") {
		return "503"
	}
	if strings.Contains(msg, "500") {
		return "500"
	}
	return "error"
}

func parseObjectKeyBounds(key string) (partition uint16, minSeq, maxSeq uint64, ok bool) {
	idx := strings.LastIndexByte(key, '/')
	base := key
	if idx >= 0 {
		base = key[idx+1:]
	}
	if !strings.HasPrefix(base, "p") || !strings.HasSuffix(base, ".parquet") {
		return 0, 0, 0, false
	}
	trimmed := strings.TrimSuffix(strings.TrimPrefix(base, "p"), ".parquet")
	parts := strings.Split(trimmed, "_")
	if len(parts) != 3 {
		return 0, 0, 0, false
	}
	pVal, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return 0, 0, 0, false
	}
	minVal, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	maxVal, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}
	return uint16(pVal), minVal, maxVal, true
}

// s3RangeReaderAt implements io.ReaderAt over S3 GetObject Range requests,
// caching the tail footer bytes so parquet.OpenFile completes in a single GET.
type s3RangeReaderAt struct {
	ctx       context.Context
	client    API
	bucket    string
	key       string
	size      int64
	mu        sync.Mutex
	tailStart int64
	tailBuf   []byte
}

func (r *s3RangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	wantEnd := off + int64(len(p))
	if wantEnd > r.size {
		wantEnd = r.size
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tailBuf == nil {
		tailStart := r.size - defaultFooterTailBytes
		if tailStart < 0 {
			tailStart = 0
		}
		if off < tailStart {
			tailStart = off
		}
		buf, err := r.fetchRange(tailStart, r.size-1)
		if err != nil {
			return 0, err
		}
		r.tailStart = tailStart
		r.tailBuf = buf
	}

	if off >= r.tailStart && wantEnd <= r.tailStart+int64(len(r.tailBuf)) {
		n := copy(p, r.tailBuf[off-r.tailStart:wantEnd-r.tailStart])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	buf, err := r.fetchRange(off, wantEnd-1)
	if err != nil {
		return 0, err
	}
	n := copy(p, buf)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *s3RangeReaderAt) fetchRange(start, end int64) ([]byte, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	out, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = out.Body.Close()
	}()
	return io.ReadAll(out.Body)
}
