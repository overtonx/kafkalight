package kafkalight

import "context"

// Handler processes a single Kafka message.
// Returning a non-nil error signals that the message was not processed
// successfully and will trigger the configured retry logic.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc is a function adapter that implements Handler.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}
