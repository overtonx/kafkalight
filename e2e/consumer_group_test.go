//go:build e2e

package e2e_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kafkalight/v4"
)

// G6-01: 2 consumers, same group, 2 partitions, 20 messages.
// Each message must be processed exactly once across both consumers.
func TestG6_01_LoadBalanced(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g6-01")
	group := uniqueGroup()
	createTopic(t, addr, topic, 2)

	const n = 20
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("x")}
	}
	produce(t, addr, topic, msgs)

	var total atomic.Int32

	newC := func() *kafkalight.Consumer {
		c := newTestConsumer(t, addr, group)
		c.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
			total.Add(1)
			return nil
		})
		return c
	}

	c1 := newC()
	c2 := newC()
	stop1 := runConsumer(context.Background(), c1)
	defer stop1()
	stop2 := runConsumer(context.Background(), c2)
	defer stop2()

	require.Eventually(t, func() bool { return total.Load() >= n }, 60*time.Second, 50*time.Millisecond)
	// Allow brief window to catch duplicates.
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(n), total.Load(), "each message must be processed exactly once")
}

// G6-02: 2 consumers, same group, 1 partition.
// Only one consumer is active; after it stops, the other picks up.
func TestG6_02_Failover(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g6-02")
	group := uniqueGroup()
	createTopic(t, addr, topic, 1)

	// Produce first batch.
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("batch1-1")},
		{Value: []byte("batch1-2")},
	})

	var total atomic.Int32

	c1 := newTestConsumer(t, addr, group)
	c1.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		total.Add(1)
		return nil
	})
	stop1 := runConsumer(context.Background(), c1)

	require.Eventually(t, func() bool { return total.Load() >= 2 }, 30*time.Second, 50*time.Millisecond)
	stop1() // Stop first consumer.

	// Start second consumer with same group.
	c2 := newTestConsumer(t, addr, group)
	c2.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		total.Add(1)
		return nil
	})
	stop2 := runConsumer(context.Background(), c2)
	defer stop2()

	// Produce more messages — must be picked up by c2.
	produce(t, addr, topic, []kafka.Message{
		{Value: []byte("batch2-1")},
		{Value: []byte("batch2-2")},
	})

	require.Eventually(t, func() bool { return total.Load() >= 4 }, 30*time.Second, 50*time.Millisecond)
}

// G6-05: 2 different consumer groups on the same topic both receive all messages.
func TestG6_05_TwoGroups(t *testing.T) {
	t.Parallel()
	addr := sharedBroker(t)
	topic := uniqueTopic("g6-05")
	group1 := uniqueGroup()
	group2 := uniqueGroup()
	createTopic(t, addr, topic, 1)

	const n = 5
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("m")}
	}
	produce(t, addr, topic, msgs)

	var count1, count2 atomic.Int32

	c1 := newTestConsumer(t, addr, group1)
	c1.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count1.Add(1)
		return nil
	})
	stop1 := runConsumer(context.Background(), c1)
	defer stop1()

	c2 := newTestConsumer(t, addr, group2)
	c2.RegisterFunc(topic, func(_ context.Context, _ *kafkalight.Message) error {
		count2.Add(1)
		return nil
	})
	stop2 := runConsumer(context.Background(), c2)
	defer stop2()

	require.Eventually(t, func() bool { return count1.Load() >= n && count2.Load() >= n }, 60*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(n), count1.Load(), "group1 must receive all messages")
	assert.Equal(t, int32(n), count2.Load(), "group2 must receive all messages")
}
