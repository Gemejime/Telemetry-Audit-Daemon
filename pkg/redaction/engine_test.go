package redaction

import (
	"bytes"
	"testing"

	"rag-telemetry-audit-daemon/pkg/pool"
)

func TestRedactInto_Patterns(t *testing.T) {
	eng := NewEngine(pool.NewBytePool(4096, 65536))

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "clean_text",
			in:   `{"query":"rag architecture overview","doc_id":42}`,
			want: `{"query":"rag architecture overview","doc_id":42}`,
		},
		{
			name: "email",
			in:   `user alice.smith+rag@corp-example.com queried docs`,
			want: `user [REDACTED] queried docs`,
		},
		{
			name: "ssn",
			in:   `patient ssn 123-45-6789 on file`,
			want: `patient ssn [REDACTED] on file`,
		},
		{
			name: "credit_card_luhn_valid",
			in:   `card=4532-0151-1283-0366 charged`,
			want: `card=[REDACTED] charged`,
		},
		{
			name: "credit_card_luhn_invalid_not_redacted",
			in:   `trace=4532-0151-1283-0367 ok`,
			want: `trace=4532-0151-1283-0367 ok`,
		},
		{
			name: "phone_formatted",
			in:   `call +1-555-867-5309 or (555) 234-5678`,
			want: `call [REDACTED] or [REDACTED]`,
		},
		{
			name: "ipv4",
			in:   `client_ip=192.168.10.45 port=443`,
			want: `client_ip=[REDACTED] port=443`,
		},
		{
			name: "jwt",
			in:   `token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c done`,
			want: `token=[REDACTED] done`,
		},
		{
			name: "bearer_and_auth_header",
			in:   `Authorization: Bearer super_secret_tok123`,
			want: `Authorization: Bearer [REDACTED]`,
		},
		{
			name: "json_secrets",
			in:   `{"api_key":"key_abc123456","password": "my-secret-pass","ok":true}`,
			want: `{"api_key":"[REDACTED]","password": "[REDACTED]","ok":true}`,
		},
		{
			name: "aws_access_key",
			in:   `key AKIAIOSFODNN7EXAMPLE used`,
			want: `key [REDACTED] used`,
		},
		{
			name: "multi_hit_chunk",
			in:   `{"actor":"bob@example.org","Authorization":"Bearer abcdef12345","ssn":"123-45-6789","ip":"10.0.0.1"}`,
			want: `{"actor":"[REDACTED]","Authorization":"Bearer [REDACTED]","ssn":"[REDACTED]","ip":"[REDACTED]"}`,
		},
		{
			name: "auth_basic_header",
			in:   `Authorization: Basic dXNlcjpwYXNz`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "auth_basic_json",
			in:   `{"Authorization":"Basic dXNlcjpwYXNz"}`,
			want: `{"Authorization":"[REDACTED]"}`,
		},
		{
			name: "json_secret_with_newlines",
			in:   "{\"api_key\"\n  :\n  \"key_abc123456\"}",
			want: "{\"api_key\"\n  :\n  \"[REDACTED]\"}",
		},
		{
			name: "password_with_punctuation",
			in:   `password=p@ss!w0rd#123 user=alice`,
			want: `password=[REDACTED] user=alice`,
		},
		{
			name: "email_trailing_period",
			in:   `Contact alice@example.com. Next`,
			want: `Contact [REDACTED]. Next`,
		},
		{
			name: "pan_followed_by_unrelated_digits",
			in:   `card 4532-0151-1283-0366 999`,
			want: `card [REDACTED] 999`,
		},
		{
			name: "phone_repeated_dashes_bounded",
			in:   `id=1----------------------------------------2`,
			want: `id=1----------------------------------------2`,
		},
	}

	dst := make([]byte, 0, 1024)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := eng.RedactInto(dst, []byte(tc.in))
			if string(got) != tc.want {
				t.Fatalf("got %q, want %q", string(got), tc.want)
			}
		})
	}
}

func TestRedactInPlace_And_WithRedacted(t *testing.T) {
	eng := NewEngine(nil)

	buf := []byte(`contact alice@example.com with api_key="secret999"`)
	got := eng.RedactInPlace(buf)
	want := []byte(`contact [REDACTED] with api_key="[REDACTED]"`)
	if !bytes.Equal(got, want) {
		t.Fatalf("RedactInPlace got %q, want %q", got, want)
	}

	err := eng.WithRedacted([]byte(`Bearer tok_xyz_123`), func(redacted []byte) error {
		if string(redacted) != `Bearer [REDACTED]` {
			t.Fatalf("WithRedacted got %q", string(redacted))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithRedacted returned error: %v", err)
	}
}

func BenchmarkRedactInto_JSONMultiHit(b *testing.B) {
	eng := NewEngine(pool.NewBytePool(4096, 65536))
	payload := []byte(`{"tenant_id":"tenant-corp-01","actor":"alice.smith@corp.example.com","Authorization":"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.sig123456","ssn":"123-45-6789","ip":"10.24.12.8","chunk":"Retrieving RAG context for +1-555-867-5309 with api_key=my_secret_key_999"}`)
	dst := make([]byte, 0, len(payload)+64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = eng.RedactInto(dst, payload)
	}
}

func BenchmarkRedactInPlace_NaturalLanguage(b *testing.B) {
	eng := NewEngine(pool.NewBytePool(4096, 65536))
	template := []byte("User bob@example.org (192.168.1.10) submitted query about AKIAIOSFODNN7EXAMPLE and phone (555) 234-5678 in RAG knowledge base.")
	buf := make([]byte, len(template))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(buf, template)
		_ = eng.RedactInPlace(buf)
	}
}
