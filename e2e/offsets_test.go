//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kafkalight/v4"
)

// G4-01: no redelivery after successful processing and restart.
func TestG4_01_NoRedeliveryAfterSuccess(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g4-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const n = 5
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: fmt.Appendf(nil, "msg-%d", i)}
	}
	produce(t, addr, topic, msgs)

	var count1 atomic.Int32
	c1 := newTestConsumer(t, addr, group)
	c1.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count1.Add(1)
		return nil
	})
	stop1 := runConsumer(context.Background(), c1)
	require.Eventually(t, func() bool { return count1.Load() >= n }, 30*time.Second, 50*time.Millisecond)
	stop1()

	// Restart consumer with same group.
	var count2 atomic.Int32
	c2 := newTestConsumer(t, addr, group)
	c2.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count2.Add(1)
		return nil
	})
	stop2 := runConsumer(context.Background(), c2)
	time.Sleep(2 * time.Second)
	stop2()

	assert.Equal(t, int32(0), count2.Load(), "no messages redelivered after clean shutdown")
}

// G4-04: exactly-once processing count after crash before commit.
// Produce 10 messages, process first 5, force-stop, restart → total = 10.
func TestG4_04_ExactlyOnceCount(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g4-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const total = 10
	msgs := make([]kafka.Message, total)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: fmt.Appendf(nil, "msg-%d", i)}
	}
	produce(t, addr, topic, msgs)

	// First consumer: stop after processing 5 (committed).
	var count1 atomic.Int32
	c1 := newTestConsumer(t, addr, group)
	c1.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count1.Add(1)
		return nil
	})
	stop1 := runConsumer(context.Background(), c1)
	require.Eventually(t, func() bool { return count1.Load() >= 5 }, 30*time.Second, 50*time.Millisecond)
	stop1()

	remaining := int32(total) - count1.Load()

	// Second consumer: processes remaining messages.
	var count2 atomic.Int32
	c2 := newTestConsumer(t, addr, group)
	c2.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count2.Add(1)
		return nil
	})
	stop2 := runConsumer(context.Background(), c2)
	require.Eventually(t, func() bool { return count2.Load() >= remaining }, 30*time.Second, 50*time.Millisecond)
	// Allow a bit more time to catch any duplicates.
	time.Sleep(1 * time.Second)
	stop2()

	totalProcessed := count1.Load() + count2.Load()
	assert.Equal(t, int32(total), totalProcessed, "total processing count must equal number of produced messages")
}

// G4-05: StartOffset=FirstOffset reads from the beginning even if messages already exist.
func TestG4_05_FirstOffset(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g4-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	// Produce messages BEFORE starting the consumer.
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("old-1")},
		{Value: []byte("old-2")},
		{Value: []byte("old-3")},
	})

	var count atomic.Int32
	c := newTestConsumer(t, addr, group,
		kafkalight.WithStartOffset(kafka.FirstOffset),
	)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return count.Load() >= 3 }, 30*time.Second, 50*time.Millisecond,
		"consumer with FirstOffset must process pre-existing messages")
}

// G4-06: StartOffset=LastOffset skips pre-existing messages, only reads new ones.
func TestG4_06_LastOffset(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g4-06")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	// Produce messages BEFORE starting the consumer.
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("old-1")},
		{Value: []byte("old-2")},
	})

	var values [][]byte
	var mu sync.Mutex
	c, err := kafkalight.New(
		kafkalight.WithBrokers(addr),
		kafkalight.WithGroupID(group),
		kafkalight.WithStartOffset(kafka.LastOffset),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	require.NoError(t, err)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		mu.Lock()
		values = append(values, msg.Value)
		mu.Unlock()
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	// Give consumer time to subscribe at LastOffset.
	time.Sleep(8 * time.Second)

	// Now produce new messages.
	produce(t, addr, topic, []kafka.Message{{Value: []byte("new-1")}})

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(values) >= 1
	}, 30*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, v := range values {
		assert.NotEqual(t, []byte("old-1"), v, "old messages must not be redelivered with LastOffset")
		assert.NotEqual(t, []byte("old-2"), v, "old messages must not be redelivered with LastOffset")
	}
}
