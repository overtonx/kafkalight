package kafkalight

import (
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

const (
	defaultReadTimeout = time.Second * 10
)

type MessageHandler func(msg *Message) error
type Middleware func(MessageHandler) MessageHandler

type KafkaRouter struct {
	mu           sync.RWMutex
	routes       map[string]MessageHandler
	middlewares  []Middleware
	topics       []string
	errorHandler ErrorHandler
	readTimeout  time.Duration
	logger       *zap.Logger
}

func NewRouter(opts ...Option) *KafkaRouter {
	router := &KafkaRouter{
		routes:      make(map[string]MessageHandler),
		readTimeout: defaultReadTimeout,
		logger:      zap.NewNop(),
	}

	for _, opt := range opts {
		opt(router)
	}

	router.errorHandler = errorHandler(router.logger)

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
	// @todo replace confluent to segmentio
	// @todo provide and processing context

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
			r.errorHandler(fmt.Errorf("topic not found in message"))
			continue
		}

		kafkaMsg, err := convertKafkaMessageToStruct(msg)
		if err != nil {
			r.errorHandler(fmt.Errorf("error converting Kafka message: %v", err))
			continue
		}

		r.mu.RLock()
		handler, exists := r.routes[*msg.TopicPartition.Topic]
		r.mu.RUnlock()

		if !exists {
			r.errorHandler(fmt.Errorf("no handler found for topic: %s", *msg.TopicPartition.Topic))
			continue
		}

		// @todo create context with trace info
		if err := handler(kafkaMsg); err != nil {
			r.errorHandler(fmt.Errorf("error handling message: %v", err))
		}
	}
}
