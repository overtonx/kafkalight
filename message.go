package kafkalight

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Header represents a single Kafka message header.
type Header struct {
	Key   string
	Value []byte
}

// Message is a wrapper around kafka.Message that is passed to handlers.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Time      time.Time

	raw kafka.Message
}

func newMessage(m kafka.Message) *Message {
	headers := make([]Header, len(m.Headers))
	for i, h := range m.Headers {
		headers[i] = Header{Key: h.Key, Value: h.Value}
	}
	return &Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
		Time:      m.Time,
		raw:       m,
	}
}

// Raw returns the underlying kafka.Message.
func (m *Message) Raw() kafka.Message {
	return m.raw
}

// HeaderValue returns the value of the first header with the given key, or nil.
func (m *Message) HeaderValue(key string) []byte {
	for _, h := range m.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}
