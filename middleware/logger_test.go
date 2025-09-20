package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/overtonx/kafkalight"
)

func TestLogger(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	logger := zap.New(core)

	key, _ := kafkalight.NewKey("test-key")

	msg := &kafkalight.Message{
		TopicPartition: kafkalight.TopicPartition{
			Topic:     "test-topic",
			Partition: 1,
		},
		Key: *key,
	}

	t.Run("success", func(t *testing.T) {
		handler := kafkalight.MessageHandler(func(ctx context.Context, msg *kafkalight.Message) error {
			return nil
		})

		loggerMiddleware := Logger(logger)
		chain := loggerMiddleware(handler)

		err := chain(context.Background(), msg)
		assert.NoError(t, err)

		logs := recorded.TakeAll()
		assert.Len(t, logs, 1)
		log := logs[0]
		assert.Equal(t, "processed message", log.Message)
		ctx := log.ContextMap()
		assert.Equal(t, "success", ctx["status"])
		assert.Equal(t, "test-topic", ctx["topic"])
		assert.Equal(t, "test-key", ctx["key"])
	})

	t.Run("error", func(t *testing.T) {
		expectedErr := errors.New("handler error")
		handler := kafkalight.MessageHandler(func(ctx context.Context, msg *kafkalight.Message) error {
			return expectedErr
		})

		loggerMiddleware := Logger(logger)
		chain := loggerMiddleware(handler)

		err := chain(context.Background(), msg)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		logs := recorded.TakeAll()
		assert.Len(t, logs, 1)
		log := logs[0]
		assert.Equal(t, "processed message", log.Message)
		ctx := log.ContextMap()
		assert.Equal(t, "error", ctx["status"])
		assert.Equal(t, "handler error", ctx["error"])
	})
}
