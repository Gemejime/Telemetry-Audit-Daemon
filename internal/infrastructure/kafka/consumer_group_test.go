package kafka

import (
	"testing"

	"rag-telemetry-audit-daemon/internal/application/command"
)

func TestNewConsumerGroup_RejectsSASLWithoutTLS(t *testing.T) {
	proc := command.NewProcessKafkaBatch(nil, nil, nil, nil, nil, nil, nil, nil, nil)

	_, err := NewConsumerGroup(ConsumerConfig{
		Brokers:      []string{"localhost:9092"},
		TLSEnabled:   false,
		SASLUsername: "user",
		SASLPassword: "secret",
		Processor:    proc,
	})
	if err == nil {
		t.Fatal("expected error when SASL credentials are configured without TLS")
	}

	_, err = NewConsumerGroup(ConsumerConfig{
		Brokers:       []string{"localhost:9092"},
		TLSEnabled:    true,
		SASLMechanism: "SCRAM-SHA-999",
		SASLUsername:  "user",
		SASLPassword:  "secret",
		Processor:     proc,
	})
	if err == nil {
		t.Fatal("expected error for unsupported SASL mechanism with TLS enabled")
	}

	cg, err := NewConsumerGroup(ConsumerConfig{
		Brokers:       []string{"localhost:9092"},
		TLSEnabled:    true,
		SASLMechanism: "PLAIN",
		SASLUsername:  "user",
		SASLPassword:  "secret",
		Processor:     proc,
	})
	if err != nil {
		t.Fatalf("expected valid SASL+TLS config to succeed, got: %v", err)
	}
	cg.Close()
}
