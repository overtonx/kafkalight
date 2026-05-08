//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/kafkalight/v4"
)

// package-level broker shared across all tests in the package.
var (
	brokerOnce sync.Once
	brokerAddr string
)

// testSem limits the number of concurrently executing E2E tests to avoid
// overwhelming the shared Redpanda container (which causes slow commits,
// slow group-joins, and cascading timeouts).
var testSem = make(chan struct{}, 8)

// sharedBroker returns the package-level Redpanda broker address.
// The container is created once per test binary run.
// It also acquires a slot in the global semaphore for the calling test,
// blocking until capacity is available (limits concurrent Redpanda load).
func sharedBroker(t *testing.T) string {
	t.Helper()
	// Throttle concurrent tests to avoid overwhelming the shared Redpanda.
	testSem <- struct{}{}
	t.Cleanup(func() { <-testSem })

	brokerOnce.Do(func() {
		ctx := context.Background()
		c, err := redpanda.Run(ctx,
			"docker.redpanda.com/redpandadata/redpanda:v24.1.1",
		)
		if err != nil {
			t.Fatalf("start redpanda: %v", err)
		}
		addr, err := c.KafkaSeedBroker(ctx)
		if err != nil {
			t.Fatalf("kafka seed broker: %v", err)
		}
		brokerAddr = addr
	})
	return brokerAddr
}

// uniqueTopic returns a unique topic name derived from base to isolate tests.
func uniqueTopic(base string) string {
	return fmt.Sprintf("%s-%s", base, uuid.New().String()[:8])
}

// uniqueGroup returns a unique consumer group ID.
func uniqueGroup() string {
	return "grp-" + uuid.New().String()[:8]
}

// createTopic creates a Kafka topic via the Admin API.
// Dials the broker directly (no controller routing) which is compatible
// with single-node Redpanda used in E2E tests.
func createTopic(t *testing.T, addr, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "exist") && !strings.Contains(msg, "already") {
			t.Logf("createTopic %q: %v (relying on auto-create)", topic, err)
		}
	}
	// Brief wait for topic metadata to propagate inside Redpanda.
	time.Sleep(200 * time.Millisecond)
}

// produce writes msgs to topic synchronously.
func produce(t *testing.T, addr, topic string, msgs []kafka.Message) {
	t.Helper()
	w := &kafka.Writer{
		Addr:                   kafka.TCP(addr),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, w.WriteMessages(ctx, msgs...))
}

// consumeN reads exactly n raw messages from topic using a dedicated reader.
// groupID should be unique so it doesn't interfere with the consumer under test.
func consumeN(t *testing.T, addr, topic, groupID string, n int, timeout time.Duration) []kafka.Message {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
	})
	defer r.Close()

	deadline := time.Now().Add(timeout)
	var out []kafka.Message
	for len(out) < n {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("consumeN(%s): timeout after %v, got %d/%d", topic, timeout, len(out), n)
		}
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		msg, err := r.FetchMessage(ctx)
		cancel()
		if err != nil {
			t.Fatalf("consumeN(%s): %v", topic, err)
		}
		require.NoError(t, r.CommitMessages(context.Background(), msg))
		out = append(out, msg)
	}
	return out
}

// waitCount blocks until counter reaches n or timeout elapses.
func waitCount(t *testing.T, counter *int, mu *sync.Mutex, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		mu.Lock()
		cur := *counter
		mu.Unlock()
		if cur >= n {
			return
		}
		if time.Now().After(deadline) {
			mu.Lock()
			final := *counter
			mu.Unlock()
			t.Fatalf("waitCount: timeout after %v, got %d/%d", timeout, final, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// newTestConsumer creates a Consumer configured for tests with fast retries.
func newTestConsumer(t *testing.T, addr, groupID string, opts ...kafkalight.Option) *kafkalight.Consumer {
	t.Helper()
	base := []kafkalight.Option{
		kafkalight.WithBrokers(addr),
		kafkalight.WithGroupID(groupID),
		kafkalight.WithStartOffset(kafka.FirstOffset),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     50 * time.Millisecond,
			Multiplier:   2.0,
			Jitter:       0,
		}),
	}
	c, err := kafkalight.New(append(base, opts...)...)
	require.NoError(t, err)
	return c
}

// runConsumer starts c.Run in a goroutine and returns a cancel func that
// calls Shutdown and waits for Run to return (with a 30s safety timeout).
func runConsumer(ctx context.Context, c *kafkalight.Consumer) (stop func()) {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- c.Run(runCtx)
	}()
	return func() {
		c.Shutdown()
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			// Run is stuck (likely a slow commit under load); don't hang the test.
		}
	}
}
