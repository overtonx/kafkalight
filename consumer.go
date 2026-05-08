package kafkalight

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// kafkaReader is the minimal interface used by the consumer.
// *kafka.Reader satisfies this interface; tests can inject a mock.
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// readerFactory creates a kafkaReader for the given topic.
// The default factory creates a real *kafka.Reader.
type readerFactory func(cfg Config, topic string) kafkaReader

func defaultReaderFactory(cfg Config, topic string) kafkaReader {
	rcfg := kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          topic,
		StartOffset:    cfg.StartOffset,
		CommitInterval: cfg.CommitInterval,
		Logger:         zapKafkaLogger{l: cfg.Logger, level: "debug"},
		ErrorLogger:    zapKafkaLogger{l: cfg.Logger, level: "error"},
	}
	return kafka.NewReader(rcfg)
}

// zapKafkaLogger bridges segmentio/kafka-go log calls to zap.
type zapKafkaLogger struct {
	l     *zap.Logger
	level string
}

func (z zapKafkaLogger) Printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if z.level == "error" {
		z.l.Error(msg, zap.String("source", "kafka-go"))
	} else {
		z.l.Debug(msg, zap.String("source", "kafka-go"))
	}
}

// Consumer reads messages from Kafka and dispatches them to registered handlers.
//
// Usage:
//
//	c, err := kafkalight.New(
//	    kafkalight.WithBrokers("localhost:9092"),
//	    kafkalight.WithGroupID("my-service"),
//	)
//	c.Register("orders", myHandler)
//	c.Run(ctx)
type Consumer struct {
	cfg           Config
	router        *router
	metrics       *metrics
	readers       []kafkaReader
	readerFactory readerFactory
	log           *zap.Logger

	running   atomic.Bool
	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

// New creates a Consumer with the supplied options.
// It validates the configuration and initialises OTel metrics.
func New(opts ...Option) (*Consumer, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	m, err := newMetrics(resolveMeterProvider(cfg.MeterProvider))
	if err != nil {
		return nil, fmt.Errorf("kafkalight: init metrics: %w", err)
	}

	// Prepend built-in middlewares: Recovery → Logging → Metrics.
	builtin := []Middleware{
		RecoveryMiddleware(),
		LoggingMiddleware(cfg.Logger),
		MetricsMiddleware(m),
	}
	cfg.GlobalMiddlewares = append(builtin, cfg.GlobalMiddlewares...)

	return &Consumer{
		cfg:           cfg,
		router:        newRouter(),
		metrics:       m,
		log:           cfg.Logger,
		done:          make(chan struct{}),
		readerFactory: defaultReaderFactory,
	}, nil
}

// Register adds a handler for the given topic.
// Middlewares are applied after the global ones (outermost → innermost).
// Must be called before Run; panics if Run has already been started.
func (c *Consumer) Register(topic string, h Handler, middlewares ...Middleware) {
	if c.running.Load() {
		panic("kafkalight: Register called after Run; register all topics before starting the consumer")
	}
	// Avoid slice aliasing: copy GlobalMiddlewares before appending.
	all := make([]Middleware, len(c.cfg.GlobalMiddlewares), len(c.cfg.GlobalMiddlewares)+len(middlewares))
	copy(all, c.cfg.GlobalMiddlewares)
	all = append(all, middlewares...)
	c.router.Register(topic, h, all...)
}

// RegisterFunc is a convenience wrapper around Register for plain functions.
func (c *Consumer) RegisterFunc(topic string, fn HandlerFunc, middlewares ...Middleware) {
	c.Register(topic, fn, middlewares...)
}

// Run starts consuming messages from all registered topics and blocks until
// ctx is cancelled or Shutdown is called.
// Run may only be called once per Consumer; a second call returns an error.
// It returns nil on clean shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("kafkalight: Run already called on this Consumer")
	}
	defer c.running.Store(false)

	topics := c.router.topics()
	if len(topics) == 0 {
		return errors.New("kafkalight: no topics registered")
	}

	c.log.Info("consumer starting",
		zap.Strings("topics", topics),
		zap.String("group_id", c.cfg.GroupID),
		zap.Int("concurrency", c.cfg.Concurrency),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// If readerFactory panics mid-loop, close any readers already opened.
	defer func() {
		if r := recover(); r != nil {
			c.closeReaders()
			panic(r) // re-panic after cleanup
		}
	}()

	for _, topic := range topics {
		r := c.readerFactory(c.cfg, topic)
		c.readers = append(c.readers, r)
		c.wg.Add(1)
		go func(reader kafkaReader, t string) {
			defer c.wg.Done()
			c.readLoop(runCtx, reader, t)
		}(r, topic)
	}

	// Block until the parent context is done or Shutdown is called.
	select {
	case <-ctx.Done():
	case <-c.done:
	}
	cancel()

	c.log.Info("consumer shutting down, draining workers…")
	c.wg.Wait()
	c.closeReaders()
	c.log.Info("consumer stopped")
	return nil
}

// Shutdown signals the consumer to stop and returns immediately.
// Run will drain in-flight messages and return.
func (c *Consumer) Shutdown() {
	c.closeOnce.Do(func() { close(c.done) })
}

// readLoop continuously reads messages from reader and dispatches them
// to a bounded worker pool.
func (c *Consumer) readLoop(ctx context.Context, r kafkaReader, topic string) {
	sem := make(chan struct{}, c.cfg.Concurrency)

	for {
		readCtx, readCancel := context.WithTimeout(ctx, c.cfg.ReadTimeout)
		msg, err := r.FetchMessage(readCtx)
		readCancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				select {
				case <-ctx.Done():
					return
				default:
					// Fetch timeout — retry.
					continue
				}
			}
			c.log.Error("fetch message error", zap.String("topic", topic), zap.Error(err))
			// Back off and check context before retrying to avoid a tight error loop.
			t := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			continue
		}

		sem <- struct{}{} // acquire slot
		c.wg.Add(1)
		go func(m kafka.Message) {
			defer func() {
				<-sem // release slot
				c.wg.Done()
			}()
			c.processMessage(ctx, r, m)
		}(msg)
	}
}

// processMessage handles retries for a single message, then commits.
func (c *Consumer) processMessage(ctx context.Context, r kafkaReader, raw kafka.Message) {
	msg := newMessage(raw)
	log := c.log.With(
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)

	var attempts int
	var lastErr error

	policy := c.cfg.RetryPolicy
	retryErr := policy.Execute(ctx, func(ctx context.Context) error {
		attempts++
		if attempts > 1 {
			log.Warn("retrying message", zap.Int("attempt", attempts))
			c.metrics.incRetries(ctx, msg.Topic)
		}
		err := c.router.Handle(ctx, msg)
		if err != nil {
			lastErr = err
		}
		return err
	})

	if retryErr != nil {
		log.Error("message processing failed after retries",
			zap.Int("attempts", attempts),
			zap.Error(lastErr),
		)
		log.Warn("skipping failed message, committing offset")
	}

	// Commit the offset using a fresh context so cancellation of the run
	// context does not prevent acknowledgement.
	commitMessage(context.Background(), r, raw, log)
}

func commitMessage(ctx context.Context, r kafkaReader, msg kafka.Message, log *zap.Logger) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := r.CommitMessages(cctx, msg); err != nil {
		log.Error("failed to commit offset", zap.Error(err))
	}
}

func (c *Consumer) closeReaders() {
	for _, r := range c.readers {
		if err := r.Close(); err != nil {
			c.log.Error("error closing kafka reader", zap.Error(err))
		}
	}
}
