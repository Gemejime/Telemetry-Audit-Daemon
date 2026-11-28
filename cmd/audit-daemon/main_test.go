package main

import "testing"

func TestEnvHelpersReturnParseErrors(t *testing.T) {
	t.Setenv("TEST_INT_EMPTY", "")
	if got, err := envIntOrDefault("TEST_INT_EMPTY", 42); err != nil || got != 42 {
		t.Fatalf("empty envIntOrDefault = (%d, %v), want (42, nil)", got, err)
	}
	t.Setenv("TEST_INT_BAD", "not-an-int")
	if _, err := envIntOrDefault("TEST_INT_BAD", 42); err == nil {
		t.Fatal("expected parse error for invalid int")
	}

	t.Setenv("TEST_INT64_EMPTY", "")
	if got, err := envInt64OrDefault("TEST_INT64_EMPTY", 99); err != nil || got != 99 {
		t.Fatalf("empty envInt64OrDefault = (%d, %v), want (99, nil)", got, err)
	}
	t.Setenv("TEST_INT64_BAD", "abc")
	if _, err := envInt64OrDefault("TEST_INT64_BAD", 99); err == nil {
		t.Fatal("expected parse error for invalid int64")
	}

	t.Setenv("TEST_BOOL_EMPTY", "")
	if got, err := envBoolOrDefault("TEST_BOOL_EMPTY", true); err != nil || !got {
		t.Fatalf("empty envBoolOrDefault = (%v, %v), want (true, nil)", got, err)
	}
	t.Setenv("TEST_BOOL_BAD", "maybe")
	if _, err := envBoolOrDefault("TEST_BOOL_BAD", false); err == nil {
		t.Fatal("expected parse error for invalid bool")
	}
}

func TestParseProducerTenantBindings(t *testing.T) {
	got, err := parseProducerTenantBindings("prod-1=tenant-a, prod-2=tenant-b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["prod-1"] != "tenant-a" || got["prod-2"] != "tenant-b" {
		t.Fatalf("unexpected bindings: %#v", got)
	}
	if _, err := parseProducerTenantBindings("bad-entry"); err == nil {
		t.Fatal("expected error for malformed binding")
	}
}
