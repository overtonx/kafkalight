package kafkalight

import "go.opentelemetry.io/otel/propagation"

var _ propagation.TextMapCarrier = &MessageCarrier{}

// MessageCarrier implements propagation.TextMapCarrier.
// It is used to extract trace context from a message's headers.
type MessageCarrier struct {
	msg *Message
}

// NewMessageCarrier creates a new MessageCarrier.
func NewMessageCarrier(msg *Message) *MessageCarrier {
	return &MessageCarrier{msg: msg}
}

// Get returns the value associated with the given key.
func (mc *MessageCarrier) Get(key string) string {
	for _, h := range mc.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets the value associated with the given key.
func (mc *MessageCarrier) Set(key string, value string) {
	header := Header{
		Key:   key,
		Value: []byte(value),
	}
	mc.msg.Headers = append(mc.msg.Headers, header)
}

// Keys returns a slice of all keys in the carrier.
func (mc *MessageCarrier) Keys() []string {
	keys := make([]string, 0, len(mc.msg.Headers))
	for _, h := range mc.msg.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}
