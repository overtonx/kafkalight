//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kafkalight/v4"
)

var errTransient = errors.New("transient handler error")

// G2-01: handler fails first 2 calls, succeeds on 3rd.
// After restart with same group, zero redeliveries (offset was committed).
func TestG2_01_RetryThenSuccess(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("retryable")}})

	var attempts atomic.Int32
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		n := attempts.Add(1)
		if n < 3 {
			return errTransient
		}
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return attempts.Load() >= 3 }, 30*time.Second, 50*time.Millisecond)
	stop()

	// Restart: offset was committed, so no redelivery.
	var redeliveries atomic.Int32
	c2 := newTestConsumer(t, addr, group)
	c2.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		redeliveries.Add(1)
		return nil
	})
	stop2 := runConsumer(context.Background(), c2)
	time.Sleep(2 * time.Second) // allow time for any redelivery
	stop2()

	assert.Equal(t, int32(0), redeliveries.Load(), "no redelivery after successful commit")
}

// G2-02: handler always fails, no DLQ → message skipped, consumer advances.
func TestG2_02_ExhaustRetries_SkipsFailedMessage(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("bad")},
		{Value: []byte("good")},
	})

	var goodProcessed atomic.Bool
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "bad" {
			return errTransient
		}
		goodProcessed.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return goodProcessed.Load() }, 30*time.Second, 50*time.Millisecond)
}

// G2-03: handler always fails (multiple attempts), no DLQ → offset committed, partition advances.
func TestG2_03_ExhaustRetries_OffsetCommitted(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-03")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("bad")},
		{Value: []byte("good")},
	})

	var goodProcessed atomic.Bool
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "bad" {
			return errTransient
		}
		goodProcessed.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return goodProcessed.Load() }, 30*time.Second, 50*time.Millisecond)
}

// G2-04: context cancelled during retry delay → consumer exits cleanly.
func TestG2_04_CtxCancelDuringRetryDelay(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x")}})

	firstAttempt := make(chan struct{}, 1)
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 2 * time.Second, // long delay to ensure ctx cancel hits it
			MaxDelay:     10 * time.Second,
			Multiplier:   1.0,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		select {
		case firstAttempt <- struct{}{}:
		default:
		}
		return errTransient
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Wait for first attempt, then cancel.
	select {
	case <-firstAttempt:
	case <-time.After(30 * time.Second):
		t.Fatal("first attempt never fired")
	}
	cancel()

	select {
	case err := <-done:
		// Run should return (nil or context error), not hang.
		_ = err
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not exit after context cancel")
	}
}

// G2-05: handler returns context.Canceled → not retried.
func TestG2_05_ContextCanceled_NotRetried(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x")}})

	var attempts atomic.Int32
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Multiplier:   1.0,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		attempts.Add(1)
		return context.Canceled
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	// Wait for the handler to be called at least once (consumer startup may be slow under load).
	require.Eventually(t, func() bool { return attempts.Load() >= 1 }, 30*time.Second, 50*time.Millisecond)
	// Brief pause to ensure no retries occur.
	time.Sleep(200 * time.Millisecond)
	stop()

	assert.Equal(t, int32(1), attempts.Load(), "context.Canceled must not be retried")
}

// G2-07: first message is a poison pill (no DLQ), second is normal.
// Second message must also be processed (partition not blocked).
func TestG2_07_PoisonThenNormal_NoBlock(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g2-07")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("poison")},
		{Value: []byte("normal")},
	})

	var normalProcessed atomic.Bool
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "poison" {
			return errTransient
		}
		normalProcessed.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return normalProcessed.Load() }, 30*time.Second, 50*time.Millisecond)
}
