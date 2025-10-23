package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/overtonx/kafkalight"
)

func TestRecovery(t *testing.T) {
	t.Run("recovers from panic", func(t *testing.T) {
		panickingHandler := func(ctx context.Context, msg *kafkalight.Message) error {
			panic("test panic")
		}

		recoveryMiddleware := Recovery()
		handler := recoveryMiddleware(panickingHandler)

		err := handler(context.Background(), &kafkalight.Message{})

		assert.Error(t, err)
		assert.Equal(t, "panic: test panic", err.Error())
	})

	t.Run("does not interfere with non-panicking handlers", func(t *testing.T) {
		var called bool
		handler := func(ctx context.Context, msg *kafkalight.Message) error {
			called = true
			return nil
		}

		recoveryMiddleware := Recovery()
		wrappedHandler := recoveryMiddleware(handler)

		err := wrappedHandler(context.Background(), &kafkalight.Message{})

		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("preserves error from non-panicking handler", func(t *testing.T) {
		expectedErr := errors.New("test error")
		handler := func(ctx context.Context, msg *kafkalight.Message) error {
			return expectedErr
		}

		recoveryMiddleware := Recovery()
		wrappedHandler := recoveryMiddleware(handler)

		err := wrappedHandler(context.Background(), &kafkalight.Message{})

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}
