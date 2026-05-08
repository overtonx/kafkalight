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

// G9-01: handler panics → RecoveryMiddleware converts to error → exhausts retries → offset committed.
func TestG9_01_PanicRecovery(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g9-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("x")},
		{Value: []byte("next")},
	})

	var nextProcessed atomic.Bool
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  2,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Multiplier:   1.0,
		}),
	)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "x" {
			panic("boom")
		}
		nextProcessed.Store(true)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	// Panic is recovered, retries exhausted, offset committed, next message processed.
	require.Eventually(t, func() bool { return nextProcessed.Load() }, 30*time.Second, 50*time.Millisecond,
		"consumer must advance past panicking message and process next one")
}

// G9-02: middleware call order — Global A, Global B, Topic C, then handler.
func TestG9_02_MiddlewareOrder(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g9-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x")}})

	var mu sync.Mutex
	var order []string
	record := func(name string) kafkalight.Middleware {
		return func(next kafkalight.Handler) kafkalight.Handler {
			return kafkalight.HandlerFunc(func(ctx context.Context, msg *kafkalight.Message) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return next.Handle(ctx, msg)
			})
		}
	}

	done := make(chan struct{}, 1)
	c, err := kafkalight.New(
		kafkalight.WithBrokers(addr),
		kafkalight.WithGroupID(group),
		kafkalight.WithStartOffset(kafka.FirstOffset),
		kafkalight.WithGlobalMiddlewares(record("GlobalA"), record("GlobalB")),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	require.NoError(t, err)
	c.Register(topic, kafkalight.HandlerFunc(func(_ context.Context, _ *kafkalight.Message) error {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	}), record("TopicC"))

	stop := runConsumer(context.Background(), c)
	defer stop()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never called")
	}
	stop()

	mu.Lock()
	got := make([]string, len(order))
	copy(got, order)
	mu.Unlock()

	// Built-in middlewares (Recovery, Logging, Metrics) are prepended, then GlobalA, GlobalB, TopicC, handler.
	// We check only the relative order of our custom ones.
	require.GreaterOrEqual(t, len(got), 4, "expected at least 4 entries: GlobalA, GlobalB, TopicC, handler")
	last4 := got[len(got)-4:]
	assert.Equal(t, []string{"GlobalA", "GlobalB", "TopicC", "handler"}, last4,
		"middleware and handler call order must be GlobalA → GlobalB → TopicC → handler")
}

// G9-03: middleware injects a value into ctx, handler reads it.
func TestG9_03_CtxPropagation(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g9-03")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x")}})

	type ctxKey struct{}
	var gotValue atomic.Value

	injector := kafkalight.Middleware(func(next kafkalight.Handler) kafkalight.Handler {
		return kafkalight.HandlerFunc(func(ctx context.Context, msg *kafkalight.Message) error {
			ctx = context.WithValue(ctx, ctxKey{}, "injected")
			return next.Handle(ctx, msg)
		})
	})

	c, err := kafkalight.New(
		kafkalight.WithBrokers(addr),
		kafkalight.WithGroupID(group),
		kafkalight.WithStartOffset(kafka.FirstOffset),
		kafkalight.WithGlobalMiddlewares(injector),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 5 * time.Millisecond,
		}),
	)
	require.NoError(t, err)
	c.RegisterFunc(topic, func(ctx context.Context, _ *kafkalight.Message) error {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			gotValue.Store(v)
		}
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return gotValue.Load() != nil }, 30*time.Second, 50*time.Millisecond)
	assert.Equal(t, "injected", gotValue.Load().(string))
}

// G9-04: middleware returns error (short-circuit) → handler not called, retry applies to middleware error.
func TestG9_04_MiddlewareShortCircuit(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g9-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("x")},
		{Value: []byte("next")},
	})

	var handlerCalls atomic.Int32
	var middlewareCalls atomic.Int32
	var nextProcessed atomic.Bool

	shortCircuit := kafkalight.Middleware(func(next kafkalight.Handler) kafkalight.Handler {
		return kafkalight.HandlerFunc(func(ctx context.Context, msg *kafkalight.Message) error {
			if string(msg.Value) == "x" {
				middlewareCalls.Add(1)
				return fmt.Errorf("middleware error: blocked")
			}
			return next.Handle(ctx, msg)
		})
	})

	c, err := kafkalight.New(
		kafkalight.WithBrokers(addr),
		kafkalight.WithGroupID(group),
		kafkalight.WithStartOffset(kafka.FirstOffset),
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Multiplier:   1.0,
		}),
	)
	require.NoError(t, err)
	c.Register(topic, kafkalight.HandlerFunc(func(_ context.Context, msg *kafkalight.Message) error {
		if string(msg.Value) == "x" {
			handlerCalls.Add(1)
		} else {
			nextProcessed.Store(true)
		}
		return nil
	}), shortCircuit)

	stop := runConsumer(context.Background(), c)
	defer stop()

	// Offset committed after retries exhausted; next message must be processed.
	require.Eventually(t, func() bool { return nextProcessed.Load() }, 30*time.Second, 50*time.Millisecond,
		"consumer must advance past short-circuited message")
	stop()

	assert.Equal(t, int32(0), handlerCalls.Load(), "handler must not be called when middleware short-circuits")
	assert.GreaterOrEqual(t, middlewareCalls.Load(), int32(3), "middleware must be called for each retry attempt")
}

// G9-05: topic-specific middlewares must not affect other topics.
func TestG9_05_MiddlewareIsolation(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topicA := uniqueTopic("g9-05-a")
	topicB := uniqueTopic("g9-05-b")
	group := uniqueGroup()
	createTopic(t, addr, topicA, 1)
	createTopic(t, addr, topicB, 1)
	produce(t, addr, topicA, []kafka.Message{{Value: []byte("a")}})
	produce(t, addr, topicB, []kafka.Message{{Value: []byte("b")}})

	var aMiddlewareCalls, bMiddlewareCalls atomic.Int32

	makeMW := func(counter *atomic.Int32) kafkalight.Middleware {
		return func(next kafkalight.Handler) kafkalight.Handler {
			return kafkalight.HandlerFunc(func(ctx context.Context, msg *kafkalight.Message) error {
				counter.Add(1)
				return next.Handle(ctx, msg)
			})
		}
	}

	var gotA, gotB atomic.Bool
	c := newTestConsumer(t, addr, group)
	c.Register(topicA,
		kafkalight.HandlerFunc(func(_ context.Context, _ *kafkalight.Message) error {
			gotA.Store(true)
			return nil
		}),
		makeMW(&aMiddlewareCalls),
	)
	c.Register(topicB,
		kafkalight.HandlerFunc(func(_ context.Context, _ *kafkalight.Message) error {
			gotB.Store(true)
			return nil
		}),
		makeMW(&bMiddlewareCalls),
	)

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return gotA.Load() && gotB.Load() }, 30*time.Second, 50*time.Millisecond)
	stop()

	assert.GreaterOrEqual(t, aMiddlewareCalls.Load(), int32(1), "topic A middleware must have been called")
	assert.GreaterOrEqual(t, bMiddlewareCalls.Load(), int32(1), "topic B middleware must have been called")
	assert.Equal(t, aMiddlewareCalls.Load(), int32(1), "topic A middleware called only for topic A")
	assert.Equal(t, bMiddlewareCalls.Load(), int32(1), "topic B middleware called only for topic B")
}
