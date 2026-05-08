//go:build e2e

package e2e_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kafkalight/v4"
)

// G8-01: concurrency=4, 4 handlers that each sleep 200ms.
// Total wall time from first handler start must be ~200ms, not ~800ms.
func TestG8_01_ParallelExecution(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g8-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const workers = 4
	msgs := make([]kafka.Message, workers)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("w")}
	}
	produce(t, addr, topic, msgs)

	var (
		completed  atomic.Int32
		firstOnce  sync.Once
		firstStart time.Time
	)
	c := newTestConsumer(t, addr, group, kafkalight.WithConcurrency(workers))
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		firstOnce.Do(func() { firstStart = time.Now() })
		time.Sleep(200 * time.Millisecond)
		completed.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return completed.Load() >= workers }, 30*time.Second, 20*time.Millisecond)
	elapsed := time.Since(firstStart)

	assert.Equal(t, int32(workers), completed.Load())
	assert.Less(t, elapsed, 800*time.Millisecond,
		"4 concurrent handlers at 200ms each should complete in ~200ms total, got %v", elapsed)
}

// G8-02: concurrency=2, burst of 10 messages — concurrent count must never exceed 2.
func TestG8_02_SemaphoreBound(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g8-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const n = 10
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("x")}
	}
	produce(t, addr, topic, msgs)

	var (
		mu       sync.Mutex
		inFlight int
		maxSeen  int
	)
	var completed atomic.Int32

	c := newTestConsumer(t, addr, group, kafkalight.WithConcurrency(2))
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		mu.Lock()
		inFlight++
		if inFlight > maxSeen {
			maxSeen = inFlight
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
		completed.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return completed.Load() >= n }, 30*time.Second, 50*time.Millisecond)
	stop()

	mu.Lock()
	max := maxSeen
	mu.Unlock()
	assert.LessOrEqual(t, max, 2, "concurrent executions must not exceed concurrency limit")
	assert.Equal(t, int32(n), completed.Load())
}

// G8-05: handler panics, concurrency=2 → semaphore released (no deadlock), subsequent messages processed.
func TestG8_05_PanicNoDeadlock(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g8-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("panic")},
		{Value: []byte("normal")},
		{Value: []byte("normal")},
	})

	var normalCount atomic.Int32
	c := newTestConsumer(t, addr, group,
		kafkalight.WithConcurrency(2),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "panic" {
			panic("test panic")
		}
		normalCount.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	// Normal messages must be processed even after a panic.
	require.Eventually(t, func() bool { return normalCount.Load() >= 2 }, 30*time.Second, 50*time.Millisecond,
		"semaphore must be released after panic, allowing subsequent messages to be processed")
}
