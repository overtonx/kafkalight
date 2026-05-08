package kafkalight

import (
	"bytes"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestNewMessage_CopiesAllFields(t *testing.T) {
	t.Parallel()

	now := time.Now()
	km := kafka.Message{
		Topic:     "orders",
		Partition: 2,
		Offset:    42,
		Key:       []byte("key-bytes"),
		Value:     []byte("value-bytes"),
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "trace-id", Value: []byte("abc123")},
		},
		Time: now,
	}

	msg := newMessage(km)

	if msg.Topic != km.Topic {
		t.Errorf("Topic: got %q want %q", msg.Topic, km.Topic)
	}
	if msg.Partition != km.Partition {
		t.Errorf("Partition: got %d want %d", msg.Partition, km.Partition)
	}
	if msg.Offset != km.Offset {
		t.Errorf("Offset: got %d want %d", msg.Offset, km.Offset)
	}
	if !bytes.Equal(msg.Key, km.Key) {
		t.Errorf("Key: got %v want %v", msg.Key, km.Key)
	}
	if !bytes.Equal(msg.Value, km.Value) {
		t.Errorf("Value: got %v want %v", msg.Value, km.Value)
	}
	if !msg.Time.Equal(now) {
		t.Errorf("Time: got %v want %v", msg.Time, now)
	}
	if len(msg.Headers) != len(km.Headers) {
		t.Fatalf("Headers length: got %d want %d", len(msg.Headers), len(km.Headers))
	}
	for i, h := range km.Headers {
		if msg.Headers[i].Key != h.Key {
			t.Errorf("Headers[%d].Key: got %q want %q", i, msg.Headers[i].Key, h.Key)
		}
		if !bytes.Equal(msg.Headers[i].Value, h.Value) {
			t.Errorf("Headers[%d].Value: got %v want %v", i, msg.Headers[i].Value, h.Value)
		}
	}
}

func TestNewMessage_NoHeaders(t *testing.T) {
	t.Parallel()

	msg := newMessage(kafka.Message{Topic: "t", Value: []byte("v")})
	if len(msg.Headers) != 0 {
		t.Errorf("expected empty headers, got %d", len(msg.Headers))
	}
}

func TestNewMessage_HeadersAreCopied(t *testing.T) {
	t.Parallel()

	// Mutating the original kafka.Message headers after conversion must not
	// affect the Message copy.
	original := []kafka.Header{{Key: "k", Value: []byte("v")}}
	km := kafka.Message{Headers: original}
	msg := newMessage(km)

	original[0].Key = "mutated"
	if msg.Headers[0].Key == "mutated" {
		t.Error("headers were not deep-copied; mutation of source affected Message")
	}
}

func TestMessage_HeaderValue(t *testing.T) {
	t.Parallel()

	msg := &Message{
		Headers: []Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "trace-id", Value: []byte("abc123")},
		},
	}

	tests := []struct {
		key  string
		want []byte
	}{
		{"content-type", []byte("application/json")},
		{"trace-id", []byte("abc123")},
		{"x-missing", nil},
		{"", nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			got := msg.HeaderValue(tc.key)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("HeaderValue(%q): got %v want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestMessage_HeaderValue_EmptyHeaders(t *testing.T) {
	t.Parallel()

	msg := &Message{}
	if v := msg.HeaderValue("any"); v != nil {
		t.Errorf("expected nil on empty headers, got %v", v)
	}
}

func TestMessage_Raw(t *testing.T) {
	t.Parallel()

	km := kafka.Message{
		Topic:  "raw-topic",
		Offset: 99,
		Value:  []byte("raw-value"),
	}
	msg := newMessage(km)
	raw := msg.Raw()

	if raw.Topic != km.Topic {
		t.Errorf("Raw().Topic: got %q want %q", raw.Topic, km.Topic)
	}
	if raw.Offset != km.Offset {
		t.Errorf("Raw().Offset: got %d want %d", raw.Offset, km.Offset)
	}
	if !bytes.Equal(raw.Value, km.Value) {
		t.Errorf("Raw().Value: got %v want %v", raw.Value, km.Value)
	}
}
