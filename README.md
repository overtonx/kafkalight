# kafkalight

`kafkalight` — это легковесная библиотека на Go для работы с Apache Kafka. Она построена на основе `confluent-kafka-go` и предоставляет простой механизм маршрутизации для обработки сообщений из разных топиков. Библиотека интегрирована с OpenTelemetry для трассировки и Zap для логирования.

## Установка

```bash
go get github.com/overtonx/kafkalight
```

## Использование

Вот простой пример использования `kafkalight` для подписки на топик и обработки сообщений:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/overtonx/kafkalight"
)

// messageHandler - это обработчик для сообщений из Kafka.
func messageHandler(ctx context.Context, msg *kafkalight.Message) error {
	fmt.Printf("Сообщение получено из топика %s: %s\n", msg.Topic, string(msg.Value))
	// Здесь ваша логика обработки сообщения
	return nil
}

func main() {
	// Создаем новый роутер с базовой конфигурацией.
	router, err := kafkalight.NewRouter()
	if err != nil {
		log.Fatalf("Ошибка при создании роутера: %v", err)
	}

	// Регистрируем обработчик для топика "my-topic".
	router.RegisterRoute("my-topic", messageHandler)

	// Запускаем прослушивание сообщений в отдельной горутине.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		log.Println("Запуск прослушивания Kafka...")
		if err := router.StartListening(ctx); err != nil {
			log.Printf("Ошибка при прослушивании: %v", err)
		}
	}()

	// Ожидаем сигнала для завершения работы.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Получен сигнал завершения, остановка...")
	cancel() // Отменяем контекст, чтобы остановить StartListening

	// Корректно завершаем работу роутера.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := router.Close(shutdownCtx); err != nil {
		log.Fatalf("Ошибка при закрытии роутера: %v", err)
	}
	log.Println("Роутер успешно остановлен.")
}
```

## Конфигурация

Вы можете настроить роутер, передавая различные опции в `NewRouter`:

-   `WithLogger(logger *zap.Logger)`: Устанавливает кастомный логгер Zap.
-   `WithReadTimeout(timeout time.Duration)`: Устанавливает таймаут для чтения сообщений.
-   `WithErrorHandler(handler func(error))`: Устанавливает обработчик ошибок.
-   `WithConsumerConfig(cfg *kafka.ConfigMap)`: Конфигурация для consumer.

## Управление offset'ами (enable.auto.commit)

`kafkalight` автоматически определяет настройку `enable.auto.commit` из переданного `kafka.ConfigMap` и меняет поведение коммита offset'ов:

| `enable.auto.commit` | Поведение |
|---|---|
| `true` (по умолчанию) | Kafka сама периодически коммитит offset'ы. Роутер не вызывает `CommitMessage`. |
| `false` | Роутер вызывает `CommitMessage` после каждого **успешно** обработанного сообщения. Если обработчик вернул ошибку — offset не коммитится. |

### Автоматический коммит (по умолчанию)

Если `enable.auto.commit` не задан или равен `true`, управлять offset'ами не нужно — Kafka делает это сама:

```go
cfg := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "auto.offset.reset": "earliest",
    // enable.auto.commit не задан — по умолчанию true
}

router, _ := kafkalight.NewRouter(kafkalight.WithConsumerConfig(cfg))
router.RegisterRoute("my-topic", func(ctx context.Context, msg *kafkalight.Message) error {
    // Offset будет закоммичен автоматически независимо от результата
    return process(msg)
})
```

### Ручной коммит

При `enable.auto.commit: false` роутер коммитит offset только после успешной обработки. Если обработчик вернул ошибку, offset не сдвигается — сообщение будет перечитано после перезапуска consumer.

```go
cfg := &kafka.ConfigMap{
    "bootstrap.servers":  "localhost:9092",
    "group.id":           "my-group",
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": false,
}

router, _ := kafkalight.NewRouter(kafkalight.WithConsumerConfig(cfg))
router.RegisterRoute("my-topic", func(ctx context.Context, msg *kafkalight.Message) error {
    if err := process(msg); err != nil {
        // Возвращаем ошибку — offset НЕ коммитится,
        // сообщение будет обработано повторно
        return err
    }
    // Возвращаем nil — offset коммитится автоматически роутером
    return nil
})
```

> Ручной режим гарантирует семантику **at-least-once**: каждое сообщение будет обработано хотя бы один раз, даже при падении приложения во время обработки.

## Middleware

Вы можете добавлять middleware для обработки сообщений перед тем, как они попадут в основной обработчик.

```go
// Пример middleware для логирования
func loggingMiddleware(next kafkalight.MessageHandler) kafkalight.MessageHandler {
    return func(ctx context.Context, msg *kafkalight.Message) error {
        log.Printf("Получено сообщение для топика %s", msg.Topic)
        return next(ctx, msg)
    }
}

// ...
router.Use(loggingMiddleware)
router.RegisterRoute("my-topic", handler) // middleware будет применен к этому обработчику
```
