package kafkalight

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// newTestKafkaRouter creates a KafkaRouter instance with default values for testing.
// It avoids creating a real kafka.Consumer, which is not needed for testing options.
func newTestKafkaRouter() *KafkaRouter {
	return &KafkaRouter{
		consumerConfig:   &kafka.ConfigMap{},
		errorHandler:     func(error) {}, // Default no-op handler
		readTimeout:      5 * time.Second,
		logger:           zap.NewNop(),
		commitWithErrors: []error{},
	}
}

func TestWithConsumerConfig(t *testing.T) {
	cfg := &kafka.ConfigMap{"group.id": "test-group"}
	router := newTestKafkaRouter()
	option := WithConsumerConfig(cfg)
	option(router) // Apply option

	if !reflect.DeepEqual(router.consumerConfig, cfg) {
		t.Errorf("WithConsumerConfig failed: expected consumerConfig %v, got %v", cfg, router.consumerConfig)
	}
}

func TestWithErrorHandler(t *testing.T) {
	expectedErr := errors.New("test error")
	var receivedErr error
	handler := func(err error) {
		receivedErr = err
	}

	router := newTestKafkaRouter()
	option := WithErrorHandler(handler)
	option(router) // Apply option

	// Call the handler to check if it's the one we set
	router.errorHandler(expectedErr)
	if receivedErr != expectedErr {
		t.Errorf("WithErrorHandler failed: expected error handler to receive %v, got %v", expectedErr, receivedErr)
	}
}

func TestWithReadTimeout(t *testing.T) {
	timeout := 10 * time.Second
	router := newTestKafkaRouter()
	option := WithReadTimeout(timeout)
	option(router) // Apply option

	if router.readTimeout != timeout {
		t.Errorf("WithReadTimeout failed: expected readTimeout %v, got %v", timeout, router.readTimeout)
	}
}

func TestWithLogger(t *testing.T) {
	logger := zaptest.NewLogger(t)
	router := newTestKafkaRouter()
	option := WithLogger(logger)
	option(router) // Apply option

	// We can't directly compare zap.Logger instances, but we can check if it's not the default nop logger
	// and if the 'module' field is added.
	// A more robust test would involve capturing logs, but for now, checking for non-nil and type is sufficient.
	if router.logger == nil {
		t.Error("WithLogger failed: logger was not set")
	}
	// Check if the logger is wrapped with the correct field
	// This is a bit hacky, but reflects the behavior of With(zap.String("module", "kafka-light"))
	// A better way would be to use a mock logger that records calls.
	// For now, we'll just ensure it's not the original logger directly.
	// If the original logger was a Nop logger, the wrapped one will also be a Nop logger.
	// So, we'll just check if it's not the *exact* same instance, which it shouldn't be after With().
	// The `With` method returns a new logger, so comparing pointers should work.
	if router.logger == logger {
		t.Error("WithLogger failed: logger instance was not wrapped with module field")
	}
}

func TestWithCommitOnErrors(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	router := newTestKafkaRouter()
	option := WithCommitOnErrors(err1, err2)
	option(router) // Apply option

	if len(router.commitWithErrors) != 2 {
		t.Fatalf("WithCommitOnErrors failed: expected 2 errors, got %d", len(router.commitWithErrors))
	}
	if router.commitWithErrors[0] != err1 || router.commitWithErrors[1] != err2 {
		t.Errorf("WithCommitOnErrors failed: expected errors %v, got %v", []error{err1, err2}, router.commitWithErrors)
	}

	// Test appending more errors
	err3 := errors.New("error 3")
	option = WithCommitOnErrors(err3)
	option(router) // Apply option again

	if len(router.commitWithErrors) != 3 {
		t.Fatalf("WithCommitOnErrors failed: expected 3 errors after appending, got %d", len(router.commitWithErrors))
	}
	if router.commitWithErrors[2] != err3 {
		t.Errorf("WithCommitOnErrors failed: expected appended error %v, got %v", err3, router.commitWithErrors[2])
	}
}
