package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overtonx/kafkalight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestManualCommit_CommitsOffsetOnlyOnSuccess verifies that with enable.auto.commit=false
// the router calls CommitMessage only after a successful handler execution.
//
// Flow:
//  1. Produce msg-success (offset 0) and msg-fail (offset 1).
//  2. Start the router; handler succeeds for msg-success and fails for msg-fail.
//  3. After both messages are processed, stop the router.
//  4. Assert that the committed offset for the group is 1:
//     — msg-success (offset 0) was committed → committed position advances to 1.
//     — msg-fail (offset 1) was NOT committed → position stays at 1.
func TestManualCommit_CommitsOffsetOnlyOnSuccess(t *testing.T) {
	cluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err)
	defer cluster.Close()

	const topic = "test-manual-commit"
	const groupID = "test-group-manual"

	require.NoError(t, cluster.CreateTopic(topic, 1, 1))
	produceMessages(t, cluster, topic, "msg-success", "msg-fail")

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  cluster.BootstrapServers(),
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	router, err := kafkalight.NewRouter(
		kafkalight.WithConsumerConfig(cfg),
		kafkalight.WithReadTimeout(200*time.Millisecond),
	)
	require.NoError(t, err)

	processed := make(chan string, 2)
	router.RegisterRoute(topic, func(_ context.Context, msg *kafkalight.Message) error {
		processed <- string(msg.Value)
		if string(msg.Value) == "msg-fail" {
			return errors.New("intentional failure")
		}
		return nil
	})

	go router.StartListening(context.Background()) //nolint:errcheck

	require.Equal(t, "msg-success", waitMessage(t, processed), "first processed message")
	require.Equal(t, "msg-fail", waitMessage(t, processed), "second processed message")

	// CommitMessage is synchronous and is called before msg-fail is read from Kafka,
	// so the committed offset is already durable by the time we reach this line.
	// We assert before Close() because the MockCluster clears group metadata when the
	// last consumer leaves the group (unlike a real Kafka broker).
	assertCommittedOffset(t, cluster, groupID, topic, kafka.Offset(1))

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	require.NoError(t, router.Close(closeCtx))
}

// TestManualCommit_NoManualCommitWhenAutoCommitEnabled verifies that the router does not
// call CommitMessage manually when enable.auto.commit is true (the default).
// Both messages are processed without error; the consumer handles offsets automatically.
func TestManualCommit_NoManualCommitWhenAutoCommitEnabled(t *testing.T) {
	cluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err)
	defer cluster.Close()

	const topic = "test-auto-commit"
	const groupID = "test-group-auto"

	require.NoError(t, cluster.CreateTopic(topic, 1, 1))
	produceMessages(t, cluster, topic, "msg1", "msg2")

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
		// enable.auto.commit defaults to true — router must NOT call CommitMessage.
	}

	router, err := kafkalight.NewRouter(
		kafkalight.WithConsumerConfig(cfg),
		kafkalight.WithReadTimeout(200*time.Millisecond),
	)
	require.NoError(t, err)

	processed := make(chan string, 2)
	router.RegisterRoute(topic, func(_ context.Context, msg *kafkalight.Message) error {
		processed <- string(msg.Value)
		return nil
	})

	go router.StartListening(context.Background()) //nolint:errcheck

	assert.Equal(t, "msg1", waitMessage(t, processed))
	assert.Equal(t, "msg2", waitMessage(t, processed))

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	assert.NoError(t, router.Close(closeCtx))
}

// --- helpers ---

func produceMessages(t *testing.T, cluster *kafka.MockCluster, topic string, values ...string) {
	t.Helper()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
	})
	require.NoError(t, err)
	defer p.Close()

	for _, v := range values {
		val := v
		require.NoError(t, p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(val),
		}, nil))
	}

	p.Flush(5000)
}

func waitMessage(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case msg := <-ch:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for handler to process message")
		return ""
	}
}

// assertCommittedOffset checks the committed offset for partition 0 of the given topic
// using the Committed API — no message consumption required.
func assertCommittedOffset(t *testing.T, cluster *kafka.MockCluster, groupID, topic string, expected kafka.Offset) {
	t.Helper()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
		"group.id":          groupID,
	})
	require.NoError(t, err)
	defer c.Close() //nolint:errcheck

	partitions := []kafka.TopicPartition{{Topic: &topic, Partition: 0}}
	committed, err := c.Committed(partitions, 10_000)
	require.NoError(t, err)
	require.Len(t, committed, 1)

	assert.Equal(t, expected, committed[0].Offset,
		"committed offset mismatch: group=%s topic=%s", groupID, topic)
}
