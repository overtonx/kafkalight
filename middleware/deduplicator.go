package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduplicator определяет интерфейс для хранилища ключей дедупликации.
type Deduplicator interface {
	// SetIfNotExists проверяет, существует ли ключ. Если нет, он сохраняет его
	// с указанным TTL и возвращает true. Если ключ уже существует, возвращает false.
	SetIfNotExists(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

type inMemoryItem struct {
	expiresAt time.Time
}

type InMemoryDeduplicator struct {
	store map[string]inMemoryItem
	mu    sync.RWMutex
}

func NewInMemoryDeduplicator() *InMemoryDeduplicator {
	return &InMemoryDeduplicator{
		store: make(map[string]inMemoryItem),
	}
}

func (d *InMemoryDeduplicator) SetIfNotExists(_ context.Context, key string, ttl time.Duration) (bool, error) {
	if key == "" {
		return false, errors.New("deduplication key cannot be empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	item, exists := d.store[key]
	if exists && time.Now().Before(item.expiresAt) {
		delete(d.store, key)
		return false, nil
	}

	d.store[key] = inMemoryItem{
		expiresAt: time.Now().Add(ttl),
	}

	return true, nil
}

// RedisDeduplicator является реализацией Deduplicator, использующей Redis в качестве хранилища.
type RedisDeduplicator struct {
	client redis.Cmdable
}

// NewRedisDeduplicator создает новый экземпляр RedisDeduplicator.
// В качестве клиента можно передать как *redis.Client, так и redis.ClusterClient.
func NewRedisDeduplicator(client redis.Cmdable) *RedisDeduplicator {
	return &RedisDeduplicator{
		client: client,
	}
}

// SetIfNotExists реализует метод интерфейса Deduplicator.
// Он использует атомарную операцию `SET key "1" NX EX ttl` в Redis.
func (d *RedisDeduplicator) SetIfNotExists(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if key == "" {
		return false, errors.New("deduplication key cannot be empty")
	}

	// Используем SetNX, который атомарно устанавливает ключ с TTL, если он не существует.
	// Метод возвращает true, если ключ был установлен, и false, если ключ уже существовал.
	wasSet, err := d.client.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}

	return wasSet, nil
}
