package kafkalight

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func Test_errorHandler(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		err    error
		expect func(t *testing.T, logs *observer.ObservedLogs)
	}{
		{
			name: "With kafka timed out error",
			err:  kafka.NewError(kafka.ErrTimedOut, "test", false),
			expect: func(t *testing.T, logs *observer.ObservedLogs) {
				t.Helper()
				assert.Equal(t, 0, logs.Len(), "Expected no logs for timed out error")
			},
		},
		{
			name: "With kafka another error",
			err:  kafka.NewError(kafka.ErrAllBrokersDown, "test", false),
			expect: func(t *testing.T, logs *observer.ObservedLogs) {
				t.Helper()
				assert.Equal(t, 1, logs.Len(), "Expected 1 log entry for other kafka errors")
				assert.Equal(t, "handler error", logs.All()[0].Message, "Expected log message to be 'handler error'")
			},
		},
		{
			name: "With another error",
			err:  errors.New("test"),
			expect: func(t *testing.T, logs *observer.ObservedLogs) {
				t.Helper()
				assert.Equal(t, 1, logs.Len(), "Expected 1 log entry for generic errors")
				assert.Equal(t, "handler error", logs.All()[0].Message, "Expected log message to be 'handler error'")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			core, logs := observer.New(zap.ErrorLevel)
			logger := zap.New(core)
			handler := errorHandler(logger)
			handler(tc.err)

			tc.expect(t, logs)
		})
	}
}
