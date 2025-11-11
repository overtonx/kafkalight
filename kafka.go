package kafkalight

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

const (
	defaultReadTimeout = time.Second * 10
)

type MessageHandler func(ctx context.Context, msg *Message) error
type Middleware func(MessageHandler) MessageHandler
type KafkaRouter struct {
	mu             sync.RWMutex
	wg             sync.WaitGroup
	started        bool
	doneCh         chan struct{}
	routes         map[string]MessageHandler
	middlewares    []Middleware
	topics         []string
	errorHandler   ErrorHandler
	readTimeout    time.Duration
	logger         *zap.Logger
	consumer       *kafka.Consumer
	consumerConfig *kafka.ConfigMap
}

func NewRouter(opts ...Option) (*KafkaRouter, error) {
	defaultConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "default-group",
		"auto.offset.reset": "earliest",
	}

	router := &KafkaRouter{
		started:        false,
		doneCh:         make(chan struct{}),
		routes:         make(map[string]MessageHandler),
		readTimeout:    defaultReadTimeout,
		logger:         zap.NewNop(),
		consumerConfig: defaultConfig,
	}

	for _, opt := range opts {
		opt(router)
	}

	c, err := kafka.NewConsumer(router.consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	router.consumer = c
	router.errorHandler = errorHandler(router.logger)

	return router, nil
}

func (r *KafkaRouter) Use(middleware ...Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.middlewares = append(r.middlewares, middleware...)
}

// RegisterRoute registers a message handler for a specific topic.
// Middlewares are applied to the handler in reverse order to create an onion-like wrapping.
// Note: routes should be registered before calling StartListening.
func (r *KafkaRouter) RegisterRoute(topic string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := len(r.middlewares) - 1; i >= 0; i-- {
		handler = r.middlewares[i](handler)
	}

	if _, exists := r.routes[topic]; !exists {
		r.topics = append(r.topics, topic)
	}
	r.routes[topic] = handler
}

func (r *KafkaRouter) StartListening(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return fmt.Errorf("router already started")
	}

	if err := r.consumer.SubscribeTopics(r.topics, nil); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	r.started = true
	r.mu.Unlock()

	r.logger.Info("router started")
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("context done, stopping listener")
			return ctx.Err()
		case <-r.doneCh:
			r.logger.Info("done channel closed, stopping listener")
			return nil
		default:
		}

		msg, err := r.consumer.ReadMessage(r.readTimeout)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
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

		r.wg.Add(1)
		func() {
			defer r.wg.Done()
			handlerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if err := handler(handlerCtx, kafkaMsg); err != nil {
				r.errorHandler(fmt.Errorf("error handling message: %v", err))
			}
		}()
	}
}

func (r *KafkaRouter) Close(ctx context.Context) error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return fmt.Errorf("router not started")
	}
	r.started = false
	close(r.doneCh)
	r.mu.Unlock()

	r.logger.Info("shutting down, waiting for message handlers to finish")

	waitCh := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		r.logger.Info("all message handlers finished")
	case <-ctx.Done():
		r.logger.Warn("context cancelled, timed out waiting for message handlers to finish")
	}

	r.logger.Info("closing kafka consumer")
	return r.consumer.Close()
}
