package kafkalight

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Header struct {
	Key   string
	Value []byte
}

type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type TimestampType int

const (
	TimestampCreateTime TimestampType = iota
	TimestampLogAppendTime
)

type Message struct {
	TopicPartition TopicPartition
	Value          []byte
	Key            Key
	Timestamp      time.Time
	TimestampType  TimestampType
	Headers        []Header
}

type Key struct {
	data []byte
}

func NewKey(data interface{}) (*Key, error) {
	switch v := data.(type) {
	case string:
		return &Key{data: []byte(v)}, nil
	case []byte:
		return &Key{data: v}, nil
	default:
		return nil, errors.New("invalid type for key, expected string or []byte")
	}
}
func (k *Key) Bytes() []byte {
	return k.data
}

func (k *Key) String() string {
	return string(k.data)
}

func (m *Message) Bind(v interface{}) error {
	return json.Unmarshal(m.Value, v)
}

func convertKafkaMessageToStruct(kafkaMsg *kafka.Message) (*Message, error) {
	msg := &Message{
		TopicPartition: TopicPartition{
			Topic:     *kafkaMsg.TopicPartition.Topic,
			Partition: kafkaMsg.TopicPartition.Partition,
			Offset:    int64(kafkaMsg.TopicPartition.Offset),
		},
		Value:         kafkaMsg.Value,
		Timestamp:     kafkaMsg.Timestamp,
		TimestampType: TimestampType(kafkaMsg.TimestampType),
	}

	// Преобразуем заголовки в структуру пакета
	for _, header := range kafkaMsg.Headers {
		msg.Headers = append(msg.Headers, Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	key, err := NewKey(kafkaMsg.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key: %w", err)
	}
	msg.Key = *key

	return msg, nil
}
