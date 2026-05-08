//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kafkalight/v4"
)

// G11-01: 500 KB payload processed correctly without truncation.
func TestG11_01_LargePayload(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	payload := bytes.Repeat([]byte("x"), 500*1024) // 500 KB
	produce(t, addr, topic, []kafka.Message{{Value: payload}})

	var gotLen atomic.Int64
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		gotLen.Store(int64(len(msg.Value)))
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return gotLen.Load() > 0 }, 30*time.Second, 50*time.Millisecond)
	assert.Equal(t, int64(500*1024), gotLen.Load(), "payload size must be preserved exactly")
}

// G11-02: binary data with null bytes in key and value is preserved exactly.
func TestG11_02_BinaryData(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	binaryKey := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	binaryVal := []byte{0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x00}
	produce(t, addr, topic, []kafka.Message{{Key: binaryKey, Value: binaryVal}})

	var gotKey, gotVal []byte
	done := make(chan struct{}, 1)
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		gotKey = msg.Key
		gotVal = msg.Value
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("message never received")
	}

	assert.Equal(t, binaryKey, gotKey, "binary key must be preserved exactly")
	assert.Equal(t, binaryVal, gotVal, "binary value must be preserved exactly")
}

// G11-03: 30 user headers — all accessible via HeaderValue.
func TestG11_03_ManyHeaders(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-03")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const numHeaders = 30
	headers := make([]kafka.Header, numHeaders)
	for i := range headers {
		headers[i] = kafka.Header{
			Key:   fmt.Sprintf("header-%d", i),
			Value: fmt.Appendf(nil, "value-%d", i),
		}
	}
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x"), Headers: headers}})

	done := make(chan struct{}, 1)
	var gotHeaders []kafkalight.Header
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		gotHeaders = msg.Headers
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("message never received")
	}

	for i := 0; i < numHeaders; i++ {
		key := fmt.Sprintf("header-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		var found string
		for _, h := range gotHeaders {
			if h.Key == key {
				found = string(h.Value)
				break
			}
		}
		assert.Equal(t, expected, found, "header %q must be accessible", key)
	}
}

// G11-04: message with nil/empty value — handler called without panic.
func TestG11_04_NilValue(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	produce(t, addr, topic, []kafka.Message{{Key: []byte("k"), Value: nil}})

	var called atomic.Bool
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		assert.True(t, len(msg.Value) == 0, "value must be nil or empty")
		called.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return called.Load() }, 30*time.Second, 50*time.Millisecond,
		"handler must be called with nil value without panic")
}

// G11-05: message with nil key — handler called without panic.
func TestG11_05_NilKey(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	produce(t, addr, topic, []kafka.Message{{Key: nil, Value: []byte("v")}})

	var called atomic.Bool
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		assert.True(t, len(msg.Key) == 0, "key must be nil or empty")
		called.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return called.Load() }, 30*time.Second, 50*time.Millisecond,
		"handler must be called with nil key without panic")
}

// G11-06: message with a header whose key is an empty string — no panic, accessible.
func TestG11_06_EmptyHeaderKey(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g11-06")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	produce(t, addr, topic, []kafka.Message{{
		Value: []byte("v"),
		Headers: []kafka.Header{
			{Key: "", Value: []byte("empty-key-value")},
			{Key: "normal", Value: []byte("normal-value")},
		},
	}})

	done := make(chan struct{}, 1)
	var gotHeaders []kafkalight.Header
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		gotHeaders = msg.Headers
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("message never received")
	}

	// Normal header must be accessible.
	found := false
	for _, h := range gotHeaders {
		if h.Key == "normal" && strings.EqualFold(string(h.Value), "normal-value") {
			found = true
			break
		}
	}
	assert.True(t, found, "normal header must be accessible alongside empty-key header")
}
