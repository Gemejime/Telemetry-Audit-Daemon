package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	DefaultMaxOpenConns     = 8
	DefaultMaxIdleConns     = 4
	DefaultConnMaxLifetime  = 30 * time.Minute
	DefaultDialTimeout      = 5 * time.Second
	DefaultMaxRetries       = 3
	DefaultRetryBaseBackoff = 100 * time.Millisecond
	DefaultRetryMaxBackoff  = 2 * time.Second
)

// Config holds ClickHouse native TCP connection pool and retry settings.
type Config struct {
	Addr                  []string
	Database              string
	Username              string
	Password              string // #nosec G117 -- runtime credential injected via env/secret, never logged
	TLSEnabled            bool
	TLSInsecureSkipVerify bool
	MaxOpenConns          int
	MaxIdleConns          int
	ConnMaxLifetime       time.Duration
	DialTimeout           time.Duration
	MaxRetries            int
	RetryBaseBackoff      time.Duration
	RetryMaxBackoff       time.Duration
	Logger                *slog.Logger
}

// Client wraps a ClickHouse native TCP connection pool with bounded exponential backoff retries.
type Client struct {
	conn             driver.Conn
	maxRetries       int
	retryBaseBackoff time.Duration
	retryMaxBackoff  time.Duration
	logger           *slog.Logger
}

// NewClient opens a ClickHouse native TCP connection pool using clickhouse-go/v2.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Addr) == 0 {
		return nil, errors.New("at least one ClickHouse address is required")
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = DefaultMaxOpenConns
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = DefaultMaxIdleConns
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = DefaultConnMaxLifetime
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	var tlsCfg *tls.Config
	if cfg.TLSEnabled {
		tlsCfg = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.TLSInsecureSkipVerify, // #nosec G402 -- configurable for local/dev clusters
		}
	}

	conn, err := ch.Open(&ch.Options{
		Addr: cfg.Addr,
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		TLS:             tlsCfg,
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Compression: &ch.Compression{
			Method: ch.CompressionLZ4,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse native tcp pool: %w", err)
	}

	return &Client{
		conn:             conn,
		maxRetries:       cfg.MaxRetries,
		retryBaseBackoff: cfg.RetryBaseBackoff,
		retryMaxBackoff:  cfg.RetryMaxBackoff,
		logger:           cfg.Logger,
	}, nil
}

// Conn returns the underlying driver.Conn.
func (c *Client) Conn() driver.Conn {
	return c.conn
}

// PrepareBatch delegates to the underlying ClickHouse native TCP connection.
func (c *Client) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query, opts...)
}

// Query delegates read queries to the underlying ClickHouse native TCP connection.
func (c *Client) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// Ping verifies ClickHouse connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Close closes the ClickHouse connection pool.
func (c *Client) Close() error {
	return c.conn.Close()
}
