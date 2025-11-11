package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/overtonx/kafkalight"
)

// --- Mock Deduplicator ---

type MockDeduplicator struct {
	mock.Mock
}

func (m *MockDeduplicator) SetIfNotExists(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, key, ttl)
	return args.Bool(0), args.Error(1)
}

// --- Tests ---

func TestDeduplication(t *testing.T) {
	const testTTL = time.Hour
	key, _ := kafkalight.NewKey("test-key")
	msg := &kafkalight.Message{Key: *key}
	noKeyMsg := &kafkalight.Message{}

	// nextHandler будет отслеживать, был ли он вызван
	var handlerCalled bool
	nextHandler := kafkalight.MessageHandler(func(ctx context.Context, msg *kafkalight.Message) error {
		handlerCalled = true
		return nil
	})

	// Сброс перед каждым тестом
	beforeEach := func() {
		handlerCalled = false
	}

	t.Run("default extractor - new message", func(t *testing.T) {
		beforeEach()
		mockStore := new(MockDeduplicator)
		mockStore.On("SetIfNotExists", mock.Anything, "test-key", testTTL).Return(true, nil).Once()

		mw := Deduplication(mockStore, testTTL)
		handler := mw(nextHandler)

		err := handler(context.Background(), msg)

		assert.NoError(t, err)
		assert.True(t, handlerCalled, "nextHandler should be called for a new message")
		mockStore.AssertExpectations(t)
	})

	t.Run("default extractor - duplicate message", func(t *testing.T) {
		beforeEach()
		mockStore := new(MockDeduplicator)
		mockStore.On("SetIfNotExists", mock.Anything, "test-key", testTTL).Return(false, nil).Once()

		mw := Deduplication(mockStore, testTTL)
		handler := mw(nextHandler)

		err := handler(context.Background(), msg)

		assert.NoError(t, err)
		assert.False(t, handlerCalled, "nextHandler should not be called for a duplicate message")
		mockStore.AssertExpectations(t)
	})

	t.Run("message with no key", func(t *testing.T) {
		beforeEach()
		mockStore := new(MockDeduplicator) // No calls should be made to the store

		mw := Deduplication(mockStore, testTTL)
		handler := mw(nextHandler)

		err := handler(context.Background(), noKeyMsg)

		assert.NoError(t, err)
		assert.True(t, handlerCalled, "nextHandler should be called for a message with no key")
		mockStore.AssertNotCalled(t, "SetIfNotExists", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("store returns an error", func(t *testing.T) {
		beforeEach()
		expectedErr := errors.New("store failure")
		mockStore := new(MockDeduplicator)
		mockStore.On("SetIfNotExists", mock.Anything, "test-key", testTTL).Return(false, expectedErr).Once()

		mw := Deduplication(mockStore, testTTL)
		handler := mw(nextHandler)

		err := handler(context.Background(), msg)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, handlerCalled, "nextHandler should not be called when store fails")
		mockStore.AssertExpectations(t)
	})

	t.Run("custom extractor - success", func(t *testing.T) {
		beforeEach()
		mockStore := new(MockDeduplicator)
		mockStore.On("SetIfNotExists", mock.Anything, "custom-key", testTTL).Return(true, nil).Once()

		customExtractor := func(msg *kafkalight.Message) (string, error) {
			return "custom-key", nil
		}

		mw := Deduplication(mockStore, testTTL, WithKeyExtractor(customExtractor))
		handler := mw(nextHandler)

		err := handler(context.Background(), msg)

		assert.NoError(t, err)
		assert.True(t, handlerCalled, "nextHandler should be called with custom extractor")
		mockStore.AssertExpectations(t)
	})

	t.Run("custom extractor returns an error", func(t *testing.T) {
		beforeEach()
		expectedErr := errors.New("extractor failure")
		mockStore := new(MockDeduplicator)

		customExtractor := func(msg *kafkalight.Message) (string, error) {
			return "", expectedErr
		}

		mw := Deduplication(mockStore, testTTL, WithKeyExtractor(customExtractor))
		handler := mw(nextHandler)

		err := handler(context.Background(), msg)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, handlerCalled, "nextHandler should not be called when extractor fails")
		mockStore.AssertNotCalled(t, "SetIfNotExists", mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestRedisDeduplicator(t *testing.T) {
	const testTTL = time.Hour
	const testKey = "test-key"
	ctx := context.Background()

	db, mock := redismock.NewClientMock()

	deduplicator := NewRedisDeduplicator(db)

	t.Run("new key", func(t *testing.T) {
		mock.ExpectSetNX(testKey, "1", testTTL).SetVal(true)

		isNew, err := deduplicator.SetIfNotExists(ctx, testKey, testTTL)

		assert.NoError(t, err)
		assert.True(t, isNew)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("duplicate key", func(t *testing.T) {
		mock.ExpectSetNX(testKey, "1", testTTL).SetVal(false)

		isNew, err := deduplicator.SetIfNotExists(ctx, testKey, testTTL)

		assert.NoError(t, err)
		assert.False(t, isNew)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("redis error", func(t *testing.T) {
		expectedErr := errors.New("redis is down")
		mock.ExpectSetNX(testKey, "1", testTTL).SetErr(expectedErr)

		isNew, err := deduplicator.SetIfNotExists(ctx, testKey, testTTL)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, isNew)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("empty key", func(t *testing.T) {
		isNew, err := deduplicator.SetIfNotExists(ctx, "", testTTL)

		assert.Error(t, err)
		assert.Equal(t, "deduplication key cannot be empty", err.Error())
		assert.False(t, isNew)
		// Убедимся, что никакие команды не были отправлены в Redis
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
