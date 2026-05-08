package kafkalight

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// mockReader is a test double for kafkaReader.
// It serves pre-loaded messages and blocks (until ctx is done) once exhausted.
type mockReader struct {
	messages  []kafka.Message
	idx       int
	mu        sync.Mutex
	committed []kafka.Message
	closeErr  error
	closed    bool
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	if m.idx >= len(m.messages) {
		m.mu.Unlock()
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := m.messages[m.idx]
	m.idx++
	m.mu.Unlock()
	return msg, nil
}

func (m *mockReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed = append(m.committed, msgs...)
	return nil
}

func (m *mockReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

func (m *mockReader) committedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

// waitForCommit polls until at least n commits are recorded or the timeout elapses.
func waitForCommit(m *mockReader, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.committedCount() >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// newTestConsumer builds a Consumer wired for unit tests:
// – no real Kafka connection (readerFactory is always overridden in callers)
// – short ReadTimeout so empty-queue waits resolve quickly
// – single attempt (no retry delay) unless overridden
func newTestConsumer(t *testing.T, opts ...Option) *Consumer {
	t.Helper()
	defaults := []Option{
		WithBrokers("localhost:9092"),
		WithGroupID("test-group"),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1}),
		WithReadTimeout(50 * time.Millisecond),
	}
	c, err := New(append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// ---------- New validation ----------

func TestNew_MissingBrokers_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := New(WithGroupID("g"))
	if err == nil {
		t.Fatal("expected error when brokers are missing")
	}
}

func TestNew_MissingGroupID_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := New(WithBrokers("localhost:9092"))
	if err == nil {
		t.Fatal("expected error when GroupID is missing")
	}
}

func TestNew_ValidConfig_ReturnsConsumer(t *testing.T) {
	t.Parallel()

	c, err := New(WithBrokers("localhost:9092"), WithGroupID("g"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil Consumer")
	}
}

// ---------- Run with no topics ----------

func TestRun_NoTopicsRegistered_ReturnsError(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when no topics are registered")
	}
}

// ---------- Message dispatch + commit ----------

func TestRun_DispatchesMessageAndCommitsOffset(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{
		messages: []kafka.Message{
			{Topic: "test-topic", Value: []byte("hello"), Offset: 1},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	handled := make(chan struct{}, 1)
	c.RegisterFunc("test-topic", func(_ context.Context, _ *Message) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called within timeout")
	}

	if !waitForCommit(mock, 1, 2*time.Second) {
		t.Error("CommitMessages not called within timeout")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// ---------- Handler error – still commits ----------

func TestRun_HandlerError_StillCommits(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t, WithRetryPolicy(RetryPolicy{MaxAttempts: 1}))
	mock := &mockReader{
		messages: []kafka.Message{
			{Topic: "test-topic", Value: []byte("fail-me"), Offset: 2},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	processed := make(chan struct{}, 1)
	c.RegisterFunc("test-topic", func(_ context.Context, _ *Message) error {
		select {
		case processed <- struct{}{}:
		default:
		}
		return errors.New("handler failed")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called within timeout")
	}

	if !waitForCommit(mock, 1, 2*time.Second) {
		t.Errorf("CommitMessages not called; got %d commits", mock.committedCount())
	}

	cancel()
	<-runDone
}

// ---------- Retry behaviour ----------

func TestRun_HandlerRetriedMaxAttemptsTimes(t *testing.T) {
	t.Parallel()

	retryPolicy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
		Jitter:       0,
	}
	c := newTestConsumer(t, WithRetryPolicy(retryPolicy))
	mock := &mockReader{
		messages: []kafka.Message{
			{Topic: "retry-topic", Value: []byte("data"), Offset: 10},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	var callCount int32
	c.RegisterFunc("retry-topic", func(_ context.Context, _ *Message) error {
		atomic.AddInt32(&callCount, 1)
		return errors.New("always fails")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	// Wait for the single message to be fully processed (retried + committed).
	if !waitForCommit(mock, 1, 5*time.Second) {
		t.Fatal("message was never committed after retries")
	}

	cancel()
	<-runDone

	if n := atomic.LoadInt32(&callCount); n != 3 {
		t.Errorf("expected handler called 3 times (MaxAttempts), got %d", n)
	}
}

func TestRun_HandlerSucceedsOnRetry(t *testing.T) {
	t.Parallel()

	retryPolicy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
		Jitter:       0,
	}
	c := newTestConsumer(t, WithRetryPolicy(retryPolicy))
	mock := &mockReader{
		messages: []kafka.Message{
			{Topic: "retry-ok-topic", Value: []byte("ok"), Offset: 5},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	var callCount int32
	c.RegisterFunc("retry-ok-topic", func(_ context.Context, _ *Message) error {
		n := atomic.AddInt32(&callCount, 1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	if !waitForCommit(mock, 1, 5*time.Second) {
		t.Fatal("message was never committed")
	}

	cancel()
	<-runDone

	if n := atomic.LoadInt32(&callCount); n != 3 {
		t.Errorf("expected 3 handler calls (fail, fail, succeed), got %d", n)
	}
}

// ---------- Shutdown ----------

func TestShutdown_StopsConsumer(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{messages: []kafka.Message{}}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }
	c.RegisterFunc("t", func(_ context.Context, _ *Message) error { return nil })

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	// Give Run time to start its goroutines.
	time.Sleep(100 * time.Millisecond)
	c.Shutdown()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}

func TestShutdown_IsIdempotent(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{messages: []kafka.Message{}}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }
	c.RegisterFunc("t", func(_ context.Context, _ *Message) error { return nil })

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(context.Background()) }()

	time.Sleep(100 * time.Millisecond)

	// Calling Shutdown multiple times must not panic.
	c.Shutdown()
	c.Shutdown()
	c.Shutdown()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after multiple Shutdown calls")
	}
}

// ---------- Context cancellation ----------

func TestRun_ContextCancellation_StopsConsumer(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{messages: []kafka.Message{}}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }
	c.RegisterFunc("t", func(_ context.Context, _ *Message) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// ---------- RegisterFunc ----------

func TestConsumer_RegisterFunc_DispatchesAndCommits(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{
		messages: []kafka.Message{
			{Topic: "func-topic", Value: []byte("data"), Offset: 7},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	handled := make(chan struct{}, 1)
	c.RegisterFunc("func-topic", func(_ context.Context, _ *Message) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("RegisterFunc handler not called within timeout")
	}

	if !waitForCommit(mock, 1, 2*time.Second) {
		t.Error("CommitMessages not called after RegisterFunc handler succeeded")
	}
}

// ---------- Multiple topics ----------

func TestRun_MultipleTopics_EachDispatched(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mocks := map[string]*mockReader{
		"topic-a": {messages: []kafka.Message{{Topic: "topic-a", Value: []byte("a"), Offset: 1}}},
		"topic-b": {messages: []kafka.Message{{Topic: "topic-b", Value: []byte("b"), Offset: 1}}},
	}
	c.readerFactory = func(_ Config, topic string) kafkaReader { return mocks[topic] }

	handledA := make(chan struct{}, 1)
	handledB := make(chan struct{}, 1)

	c.RegisterFunc("topic-a", func(_ context.Context, _ *Message) error {
		select {
		case handledA <- struct{}{}:
		default:
		}
		return nil
	})
	c.RegisterFunc("topic-b", func(_ context.Context, _ *Message) error {
		select {
		case handledB <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case <-handledA:
	case <-time.After(5 * time.Second):
		t.Fatal("topic-a handler not called within timeout")
	}
	select {
	case <-handledB:
	case <-time.After(5 * time.Second):
		t.Fatal("topic-b handler not called within timeout")
	}
}

// ---------- Message fields forwarded to handler ----------

func TestRun_MessageFieldsForwardedToHandler(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mock := &mockReader{
		messages: []kafka.Message{
			{
				Topic:     "fields-topic",
				Partition: 3,
				Offset:    99,
				Key:       []byte("my-key"),
				Value:     []byte("my-value"),
				Headers:   []kafka.Header{{Key: "x-foo", Value: []byte("bar")}},
			},
		},
	}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }

	type captured struct {
		topic     string
		partition int
		offset    int64
		key       []byte
		value     []byte
		headerVal []byte
	}
	got := make(chan captured, 1)

	c.RegisterFunc("fields-topic", func(_ context.Context, msg *Message) error {
		got <- captured{
			topic:     msg.Topic,
			partition: msg.Partition,
			offset:    msg.Offset,
			key:       msg.Key,
			value:     msg.Value,
			headerVal: msg.HeaderValue("x-foo"),
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case c := <-got:
		if c.topic != "fields-topic" {
			t.Errorf("Topic: got %q", c.topic)
		}
		if c.partition != 3 {
			t.Errorf("Partition: got %d", c.partition)
		}
		if c.offset != 99 {
			t.Errorf("Offset: got %d", c.offset)
		}
		if string(c.key) != "my-key" {
			t.Errorf("Key: got %q", c.key)
		}
		if string(c.value) != "my-value" {
			t.Errorf("Value: got %q", c.value)
		}
		if string(c.headerVal) != "bar" {
			t.Errorf("HeaderValue: got %q", c.headerVal)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called within timeout")
	}
}

func TestRun_CalledTwice_ReturnsError(t *testing.T) {
	t.Parallel()

	c, err := New(
		WithBrokers("localhost:9092"),
		WithGroupID("test-group"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mock := &mockReader{}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }
	c.Register("topic-a", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First Run in background.
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	// Give the first Run time to set running=true.
	time.Sleep(50 * time.Millisecond)

	// Second call must return an error immediately.
	err = c.Run(ctx)
	if err == nil {
		t.Fatal("expected error from second Run call, got nil")
	}

	cancel()
	<-runDone
}

func TestRegister_AfterRun_Panics(t *testing.T) {
	t.Parallel()

	c, err := New(
		WithBrokers("localhost:9092"),
		WithGroupID("test-group"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mock := &mockReader{}
	c.readerFactory = func(_ Config, _ string) kafkaReader { return mock }
	c.Register("topic-a", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from Register after Run, got none")
		}
	}()
	c.Register("topic-b", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))
}

func TestRun_ReaderFactoryPanic_Cleanup(t *testing.T) {
	t.Parallel()

	c, err := New(
		WithBrokers("localhost:9092"),
		WithGroupID("test-group"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	closed := make([]bool, 0)
	var mu sync.Mutex

	callCount := 0
	c.readerFactory = func(_ Config, _ string) kafkaReader {
		callCount++
		if callCount == 2 {
			panic("injected factory panic")
		}
		m := &mockReader{}
		// Wrap Close to record it was called.
		_ = m
		// Return a trackable reader.
		tr := &trackingReader{inner: m, onClose: func() {
			mu.Lock()
			closed = append(closed, true)
			mu.Unlock()
		}}
		return tr
	}
	c.Register("topic-a", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))
	c.Register("topic-b", HandlerFunc(func(_ context.Context, _ *Message) error { return nil }))

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic to propagate, got none")
		}
		// Reader created before the panic must have been closed.
		mu.Lock()
		n := len(closed)
		mu.Unlock()
		if n != 1 {
			t.Errorf("expected 1 reader closed after panic, got %d", n)
		}
	}()

	c.Run(context.Background()) //nolint:errcheck
}

// trackingReader wraps kafkaReader and calls onClose when Close is invoked.
type trackingReader struct {
	inner   kafkaReader
	onClose func()
}

func (r *trackingReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return r.inner.FetchMessage(ctx)
}
func (r *trackingReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return r.inner.CommitMessages(ctx, msgs...)
}
func (r *trackingReader) Close() error {
	r.onClose()
	return r.inner.Close()
}
