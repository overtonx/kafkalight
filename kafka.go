package kafkalight

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageHandler func(msg *Message) error
type Middleware func(MessageHandler) MessageHandler

type KafkaRouter struct {
	mu           sync.RWMutex
	routes       map[string]MessageHandler
	middlewares  []Middleware
	topics       []string
	errorHandler func(error)   // Обработчик ошибок
	readTimeout  time.Duration // Время ожидания сообщения из Kafka
}

func NewRouter(opts ...Option) *KafkaRouter {
	router := &KafkaRouter{
		routes:       make(map[string]MessageHandler),
		readTimeout:  10 * time.Second,                                 // Значение по умолчанию
		errorHandler: func(err error) { log.Printf("Error: %v", err) }, // Значение по умолчанию
	}

	// Применяем все переданные опции
	for _, opt := range opts {
		opt(router)
	}

	return router
}

func (r *KafkaRouter) Use(middleware Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.middlewares = append(r.middlewares, middleware)
}

func (r *KafkaRouter) RegisterRoute(topic string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, mw := range r.middlewares {
		handler = mw(handler)
	}

	r.routes[topic] = handler
	r.topics = append(r.topics, topic)
}

func (r *KafkaRouter) StartListening(c *kafka.Consumer) error {
	r.mu.RLock()
	err := c.SubscribeTopics(r.topics, nil)
	r.mu.RUnlock()

	if err != nil {
		return err
	}

	for {
		msg, err := c.ReadMessage(r.readTimeout)
		if err != nil {
			r.errorHandler(err)
			continue
		}

		if msg.TopicPartition.Topic == nil {
			r.errorHandler(fmt.Errorf("Topic not found in message: %s", *msg.TopicPartition.Topic))
			continue
		}

		kafkaMsg, err := ConvertKafkaMessageToStruct(msg)
		if err != nil {
			r.errorHandler(fmt.Errorf("Error converting Kafka message: %v", err))
			continue
		}

		r.mu.RUnlock()
		handler, exists := r.routes[*msg.TopicPartition.Topic]
		r.mu.RUnlock()

		if !exists {
			r.errorHandler(fmt.Errorf("No handler found for topic: %s", *msg.TopicPartition.Topic))
			continue
		}

		if err := handler(kafkaMsg); err != nil {
			r.errorHandler(fmt.Errorf("Error handling message: %v", err))
		}
	}
}
