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

// G1-01: single message delivered to handler, offset committed.
func TestG1_01_SingleMessage(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g1-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{Value: []byte("hello")}})

	var called atomic.Int32
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		assert.Equal(t, []byte("hello"), msg.Value)
		called.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return called.Load() >= 1 }, 30*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(1), called.Load())
}

// G1-02: 100 messages, all processed.
func TestG1_02_HundredMessages(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g1-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const n = 100
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("msg")}
	}
	produce(t, addr, topic, msgs)

	var count atomic.Int32
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return count.Load() >= n }, 60*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(n), count.Load())
}

// G1-03: two topics, each message routed to the correct handler.
func TestG1_03_TwoTopics(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topicA := uniqueTopic("g1-03-a")
	topicB := uniqueTopic("g1-03-b")
	group := uniqueGroup()
	createTopic(t, addr, topicA, 1)
	createTopic(t, addr, topicB, 1)
	produce(t, addr, topicA, []kafka.Message{{Value: []byte("A")}})
	produce(t, addr, topicB, []kafka.Message{{Value: []byte("B")}})

	var gotA, gotB atomic.Int32
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topicA, func(_ context.Context, msg *kafkalight.Message) error {
		assert.Equal(t, []byte("A"), msg.Value)
		gotA.Add(1)
		return nil
	})
	c.RegisterFunc(topicB, func(_ context.Context, msg *kafkalight.Message) error {
		assert.Equal(t, []byte("B"), msg.Value)
		gotB.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return gotA.Load() >= 1 && gotB.Load() >= 1 }, 30*time.Second, 50*time.Millisecond)
}

// G1-04: 4-partition topic, 40 messages, all processed.
func TestG1_04_MultiPartition(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g1-04")
	group := uniqueGroup()
	createTopic(t, addr, topic, 4)

	const n = 40
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("x")}
	}
	produce(t, addr, topic, msgs)

	var count atomic.Int32
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return count.Load() >= n }, 60*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(n), count.Load())
}

// G1-05: 1000 messages with concurrency=8, all processed.
func TestG1_05_HighThroughput(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g1-05")
	group := uniqueGroup()
	createTopic(t, addr, topic, 4)

	const n = 1000
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("bulk")}
	}
	produce(t, addr, topic, msgs)

	var count atomic.Int32
	c := newTestConsumer(t, addr, group, kafkalight.WithConcurrency(8))
	c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count.Add(1)
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool { return count.Load() >= n }, 120*time.Second, 100*time.Millisecond)
	assert.Equal(t, int32(n), count.Load())
}

// G1-06: handler can read message key and headers.
func TestG1_06_MessageAttributes(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g1-06")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)
	produce(t, addr, topic, []kafka.Message{{
		Key:   []byte("mykey"),
		Value: []byte("myvalue"),
		Headers: []kafka.Header{
			{Key: "x-trace-id", Value: []byte("abc123")},
		},
	}})

	var mu sync.Mutex
	var gotKey, gotValue, gotHeader []byte
	c := newTestConsumer(t, addr, group)
	c.RegisterFunc(topic, func(_ context.Context, msg *kafkalight.Message) error {
		mu.Lock()
		gotKey = msg.Key
		gotValue = msg.Value
		gotHeader = msg.HeaderValue("x-trace-id")
		mu.Unlock()
		return nil
	})

	stop := runConsumer(context.Background(), c)
	defer stop()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotKey != nil
	}, 30*time.Second, 50*time.Millisecond)

	assert.Equal(t, []byte("mykey"), gotKey)
	assert.Equal(t, []byte("myvalue"), gotValue)
	assert.Equal(t, []byte("abc123"), gotHeader)
}
