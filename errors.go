package kafkalight

import (
	"errors"
	"fmt"
)

// Sentinel errors.
var (
	// ErrNoHandler is returned when no handler is registered for a topic.
	ErrNoHandler = errors.New("kafkalight: no handler registered for topic")

	// ErrMaxRetries is returned when all retry attempts are exhausted.
	ErrMaxRetries = errors.New("kafkalight: max retries exceeded")

	// ErrConsumerClosed is returned when operations are performed on a stopped consumer.
	ErrConsumerClosed = errors.New("kafkalight: consumer is closed")
)

// ProcessingError wraps the original handler error together with retry metadata.
type ProcessingError struct {
	Topic    string
	Offset   int64
	Attempts int
	Err      error
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("kafkalight: processing failed after %d attempt(s) on topic %q offset %d: %v",
		e.Attempts, e.Topic, e.Offset, e.Err)
}

func (e *ProcessingError) Unwrap() error { return e.Err }
