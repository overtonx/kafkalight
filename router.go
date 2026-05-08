package kafkalight

import (
	"context"
	"fmt"
	"sync"
)

// router maps Kafka topics to their handlers (with middleware already applied).
type router struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func newRouter() *router {
	return &router{handlers: make(map[string]Handler)}
}

// Register associates handler with topic.
// Middlewares are applied in left-to-right order (outermost first).
// Calling Register twice for the same topic overwrites the previous handler.
func (r *router) Register(topic string, handler Handler, middlewares ...Middleware) {
	h := Apply(handler, middlewares...)
	r.mu.Lock()
	r.handlers[topic] = h
	r.mu.Unlock()
}

// RegisterFunc is a convenience wrapper for HandlerFunc.
func (r *router) RegisterFunc(topic string, fn HandlerFunc, middlewares ...Middleware) {
	r.Register(topic, fn, middlewares...)
}

// Handle dispatches msg to the registered handler for msg.Topic.
func (r *router) Handle(ctx context.Context, msg *Message) error {
	r.mu.RLock()
	h, ok := r.handlers[msg.Topic]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %s", ErrNoHandler, msg.Topic)
	}
	return h.Handle(ctx, msg)
}

// topics returns the list of registered topic names.
func (r *router) topics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	topics := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		topics = append(topics, t)
	}
	return topics
}
