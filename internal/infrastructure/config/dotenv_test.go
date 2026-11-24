package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDotEnv_ParsesAndSetsMissingVars(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	content := `
# Comment line
KAFKA_BROKERS=broker1:9092,broker2:9092 # inline comment
export CLICKHOUSE_ADDR="ch.internal:9000"
S3_KMS_KEY_ID='alias/my-key'
EMPTY_VAR=
MULTILINE_VAR="line1\nline2"
`
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	// Pre-set variable must NOT be overridden by .env.
	t.Setenv("CLICKHOUSE_ADDR", "override:9000")
	t.Setenv("KAFKA_BROKERS", "")
	_ = os.Unsetenv("KAFKA_BROKERS")
	_ = os.Unsetenv("S3_KMS_KEY_ID")
	_ = os.Unsetenv("EMPTY_VAR")
	_ = os.Unsetenv("MULTILINE_VAR")
	t.Cleanup(func() {
		_ = os.Unsetenv("KAFKA_BROKERS")
		_ = os.Unsetenv("S3_KMS_KEY_ID")
		_ = os.Unsetenv("EMPTY_VAR")
		_ = os.Unsetenv("MULTILINE_VAR")
	})

	if err := LoadDotEnv(envPath); err != nil {
		t.Fatalf("LoadDotEnv failed: %v", err)
	}

	if got := os.Getenv("KAFKA_BROKERS"); got != "broker1:9092,broker2:9092" {
		t.Fatalf("KAFKA_BROKERS = %q, want broker1:9092,broker2:9092", got)
	}
	if got := os.Getenv("CLICKHOUSE_ADDR"); got != "override:9000" {
		t.Fatalf("CLICKHOUSE_ADDR = %q, want override:9000", got)
	}
	if got := os.Getenv("S3_KMS_KEY_ID"); got != "alias/my-key" {
		t.Fatalf("S3_KMS_KEY_ID = %q, want alias/my-key", got)
	}
	if got, ok := os.LookupEnv("EMPTY_VAR"); !ok || got != "" {
		t.Fatalf("EMPTY_VAR = (%q, %v), want (\"\", true)", got, ok)
	}
	if got := os.Getenv("MULTILINE_VAR"); got != "line1\nline2" {
		t.Fatalf("MULTILINE_VAR = %q, want line1\\nline2", got)
	}
}

func TestLoadDotEnv_MissingDefaultIsNoOp(t *testing.T) {
	dir := t.TempDir()
	// Create a dummy go.mod so findDotEnvFromCWD stops inside dir.
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n"), 0o600); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	t.Setenv("DOTENV_PATH", "")
	_ = os.Unsetenv("DOTENV_PATH")

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	if err := LoadDotEnv(); err != nil {
		t.Fatalf("expected nil when .env is missing, got %v", err)
	}
}

func TestLoadDotEnv_MalformedLineRedactsSecretValue(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte("SECRET_TOKEN=\"unterminated-super-secret\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	err := LoadDotEnv(envPath)
	if err == nil {
		t.Fatal("expected error for malformed .env")
	}
	if strings.Contains(err.Error(), "unterminated-super-secret") {
		t.Fatalf("error leaked secret value: %v", err)
	}
}
