package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const dotEnvPathEnv = "DOTENV_PATH"

// LoadDotEnv loads environment variables from a .env file into the process
// environment without overriding variables that are already set.
//
// When no explicit paths are provided, it checks DOTENV_PATH (if set) and
// otherwise searches for .env starting from the current working directory up
// to the Go module root (go.mod). A missing .env file is not an error.
func LoadDotEnv(paths ...string) error {
	if len(paths) > 0 {
		for _, p := range paths {
			if p == "" {
				continue
			}
			if err := loadFile(p, true); err != nil {
				return err
			}
		}
		return nil
	}

	if explicit := os.Getenv(dotEnvPathEnv); explicit != "" {
		return loadFile(explicit, true)
	}

	if found, ok := findDotEnvFromCWD(); ok {
		return loadFile(found, false)
	}
	return nil
}

func findDotEnvFromCWD() (string, bool) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, ".env")
		if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
			return candidate, true
		}
		// Stop walking once we reach the repository/module root.
		if _, modErr := os.Stat(filepath.Join(dir, "go.mod")); modErr == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func loadFile(path string, failIfMissing bool) error {
	f, err := os.Open(path) // #nosec G304 -- path is explicitly configured or resolved .env file
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !failIfMissing {
			return nil
		}
		return fmt.Errorf("open env file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		key, val, ok, parseErr := parseLine(scanner.Text())
		if parseErr != nil {
			// Never include the raw line or value in errors to preserve secret redaction.
			return fmt.Errorf("parse env file %q line %d: %w", path, lineNum, parseErr)
		}
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			if setErr := os.Setenv(key, val); setErr != nil {
				return fmt.Errorf("set env key %q from %q: %w", key, path, setErr)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read env file %q: %w", path, err)
	}
	return nil
}

func parseLine(raw string) (key, val string, ok bool, err error) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	if strings.HasPrefix(line, "export ") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
	}

	idx := strings.IndexByte(line, '=')
	if idx <= 0 {
		return "", "", false, errors.New("expected KEY=VALUE assignment")
	}

	key = strings.TrimSpace(line[:idx])
	if key == "" || strings.ContainsAny(key, " \t") {
		return "", "", false, errors.New("invalid environment variable name")
	}

	valPart := strings.TrimSpace(line[idx+1:])
	val, err = parseValue(valPart)
	if err != nil {
		return "", "", false, err
	}
	return key, val, true, nil
}

func parseValue(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	quote := s[0]
	if quote == '\'' || quote == '"' {
		var b strings.Builder
		escaped := false
		for i := 1; i < len(s); i++ {
			ch := s[i]
			if quote == '"' && escaped {
				switch ch {
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				case '"', '\\':
					b.WriteByte(ch)
				default:
					b.WriteByte('\\')
					b.WriteByte(ch)
				}
				escaped = false
				continue
			}
			if quote == '"' && ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				rest := strings.TrimSpace(s[i+1:])
				if rest != "" && !strings.HasPrefix(rest, "#") {
					return "", errors.New("unexpected trailing characters after closing quote")
				}
				return b.String(), nil
			}
			b.WriteByte(ch)
		}
		return "", errors.New("unterminated quoted value")
	}

	// Unquoted value: strip inline comments preceded by whitespace.
	for i := 0; i < len(s); i++ {
		if s[i] == '#' && i > 0 && (s[i-1] == ' ' || s[i-1] == '\t') {
			return strings.TrimSpace(s[:i]), nil
		}
	}
	return strings.TrimSpace(s), nil
}
