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

// G5-01: Shutdown() while a slow handler is active → handler completes, Run returns.
func TestG5_01_ShutdownDuringProcessing(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("slow")}})

	handlerStarted := make(chan struct{}, 1)
	handlerFinished := make(chan struct{}, 1)

	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		handlerStarted <- struct{}{}
		time.Sleep(300 * time.Millisecond)
		handlerFinished <- struct{}{}
		return nil
	})

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	select {
	case <-handlerStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never started")
	}

	c.Shutdown()

	select {
	case <-handlerFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete after Shutdown")
	}

	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}

// G5-02: context cancel while a slow handler is active → handler completes, Run returns.
func TestG5_02_CtxCancelDuringProcessing(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("slow")}})

	handlerStarted := make(chan struct{}, 1)
	handlerFinished := make(chan struct{}, 1)

	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		handlerStarted <- struct{}{}
		time.Sleep(300 * time.Millisecond)
		handlerFinished <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	select {
	case <-handlerStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	select {
	case <-handlerFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete after ctx cancel")
	}

	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// G5-03: Shutdown() on empty topic → Run returns quickly without hanging.
func TestG5_03_ShutdownEmptyTopic(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-03")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error { return nil })

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	// Give the consumer a moment to start, then shut down.
	time.Sleep(200 * time.Millisecond)
	c.Shutdown()

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Shutdown on empty topic")
	}
}

// G5-04: calling Shutdown() twice must not panic or deadlock.
func TestG5_04_ShutdownTwice(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error { return nil })

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	time.Sleep(200 * time.Millisecond)

	// Must not panic.
	assert.NotPanics(t, func() {
		c.Shutdown()
		c.Shutdown()
	})

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after double Shutdown")
	}
}

// G5-05: Shutdown() during retry delay → consumer exits cleanly without waiting for full delay.
func TestG5_05_ShutdownDuringRetryDelay(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("x")}})

	const retryDelay = 60 * time.Second // deliberately long so any interrupt is unambiguous

	firstAttempt := make(chan struct{}, 1)
	c := newTestConsumer(t, addr, group,
		kafkalight.WithRetryPolicy(kafkalight.RetryPolicy{
			MaxAttempts:  5,
			InitialDelay: retryDelay,
			MaxDelay:     retryDelay,
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

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	select {
	case <-firstAttempt:
	case <-time.After(30 * time.Second):
		t.Fatal("first attempt never fired")
	}

	start := time.Now()
	c.Shutdown()

	select {
	case <-runDone:
		elapsed := time.Since(start)
		// Run must return well before the 60s retry delay expires.
		// A slow commit (under load) may add a few seconds, so allow up to 30s.
		assert.Less(t, elapsed, 30*time.Second, "Run should exit before the 60s retry delay")
	case <-time.After(35 * time.Second):
		t.Fatal("Run did not return within expected time after Shutdown")
	}
}

// G5-07: many workers in-flight at shutdown — all complete, wg.Wait() does not hang.
func TestG5_07_ManyWorkersInFlight(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g5-07")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const workers = 10
	msgs := make([]kafka.Message, workers)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("w")}
	}
	produce(t, addr, topic, msgs)

	var (
		mu       sync.Mutex
		inFlight int
		maxSeen  int
	)
	var completed atomic.Int32

	c := newTestConsumer(t, addr, group, kafkalight.WithConcurrency(workers))
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		mu.Lock()
		inFlight++
		if inFlight > maxSeen {
			maxSeen = inFlight
		}
		mu.Unlock()

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
		completed.Add(1)
		return nil
	})

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	// Wait for all workers to complete.
	require.Eventually(t, func() bool { return completed.Load() >= workers }, 30*time.Second, 50*time.Millisecond)
	c.Shutdown()

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after all workers finished")
	}
	assert.Equal(t, int32(workers), completed.Load())
}
