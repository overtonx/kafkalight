# Kafkalight - Пакет для работы с Kafka в Go

Kafkalight — это Go-пакет, который упрощает подписку на топики Kafka и обработку сообщений в структурированном виде, с поддержкой middleware, обработчиков ошибок и настроек таймаутов.

## Особенности

- **Подписка на топики Kafka** и обработка сообщений через простой и гибкий API.
- **Поддержка кастомных обработчиков ошибок** с возможностью использования обработчика по умолчанию.
- **Middleware** для изменения или расширения логики обработки сообщений.
- **Конфигурируемый таймаут чтения** для consumer'а Kafka.
- **Маппинг сообщений Kafka** в структурированные типы Go с использованием JSON-десериализации.
- **Поддержка ключей и заголовков сообщений** в структурированном виде.

## Установка

Для установки пакета используйте команду `go get`:

```bash
go get github.com/overtonx/kafkalight
```

## Использование
1. Пример базового использования

Пример использования Kafkalight с кастомным обработчиком ошибок и таймаутом для чтения сообщений:
```go
package main

import (
    "fmt"
    "log"
    "github.com/overtonx/kafkalight"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "time"
)

// Кастомный обработчик ошибок
func errorHandler(err error) {
    log.Printf("Ошибка: %v", err)
}

// Структура для маппинга данных из Kafka
type MyMessage struct {
    Name  string `json:"name"`
    Value int    `json:"value"`
}

func main() {
    // Конфигурация Kafka consumer
    config := kafka.KafkaConfig{
        Brokers: "localhost:9092",
        GroupID: "my-group",
        Topics:  []string{"topic1", "topic2"},
    }

    consumer, err := kafka.NewConsumer(config)
    if err != nil {
        log.Fatalf("Ошибка при создании Kafka consumer: %v", err)
    }
    defer consumer.Close()

    // Создаем маршрутизатор с кастомным обработчиком ошибок и таймаутом
    router := kafkalight.NewRouter(
        kafkalight.WithErrorHandler(errorHandler),        // Кастомный обработчик ошибок
        kafkalight.WithReadTimeout(5*time.Second),        // Установка таймаута для Kafka consumer
    )

    // Регистрируем обработчик для "topic1"
    router.RegisterRoute("topic1", func(msg *kafkalight.Message) error {
        var myMsg MyMessage
        // Маппим Kafka сообщение в структуру
        err := msg.Bind(&myMsg)
        if err != nil {
            return err
        }
        fmt.Printf("Получено сообщение на topic1: %+v\n", myMsg)
        return nil
    })

    // Начинаем слушать сообщения из Kafka
    if err := router.StartListening(consumer); err != nil {
        log.Fatalf("Ошибка при запуске прослушивания: %v", err)
    }
}

```
## 2. Регистрация маршрутов

Для регистрации обработчиков сообщений для конкретных топиков Kafka используйте метод RegisterRoute. Обработчик получит типизированное сообщение, которое можно обрабатывать.
```go
router.RegisterRoute("topic1", func(msg *kafkalight.Message) error {
    var myMsg MyMessage
    // Маппим Kafka сообщение в структуру
    err := msg.Bind(&myMsg)
    if err != nil {
        return err
    }
    fmt.Println("Получено сообщение:", myMsg)
    return nil
})
```
## 3. Использование Middleware

Middleware позволяет изменить или расширить логику обработки сообщений. Можно применить несколько middleware для одного обработчика.
```go
// Middleware для логирования обработки сообщений
func loggingMiddleware(next kafkalight.MessageHandler) kafkalight.MessageHandler {
    return func(msg *kafkalight.Message) error {
        log.Printf("Обработка сообщения: %v", msg)
        return next(msg)
    }
}

// Применяем middleware к обработчику
router.Use(loggingMiddleware)
```
## 4. Обработка ошибок

Можно указать кастомный обработчик ошибок при создании маршрутизатора через опцию WithErrorHandler. Если обработчик не передан, используется стандартный, который логирует ошибку.
```go
// Создаем маршрутизатор с кастомным обработчиком ошибок
router := kafkalight.NewRouter(kafkalight.WithErrorHandler(func(err error) {
    log.Fatalf("Кастомная ошибка: %v", err)
}))
```
## 5. Установка таймаута для чтения сообщений

Можно установить кастомный таймаут для Kafka consumer с помощью опции WithReadTimeout.
```go
router := kafkalight.NewRouter(kafkalight.WithReadTimeout(10 * time.Second))
```
Это гарантирует, что consumer не будет блокироваться на неопределенный срок при ожидании сообщений.

## API
### KafkaRouter

- NewRouter(options ...Option): Создает новый KafkaRouter с переданными опциями.
- RegisterRoute(topic string, handler MessageHandler): Регистрирует обработчик сообщений для конкретного Kafka топика.
- Use(middleware Middleware): Добавляет middleware, который будет применяться к обработчикам сообщений.
- StartListening(consumer *kafka.Consumer): Начинает прослушивание сообщений из Kafka.

### Options

- WithErrorHandler(handler func(error)): Устанавливает кастомный обработчик ошибок.
- WithReadTimeout(timeout time.Duration): Устанавливает таймаут для Kafka consumer.

### MessageHandler

- *type MessageHandler func(msg Message) error: Тип функции, используемой для обработки сообщений из Kafka.

### Middleware

- type Middleware func(MessageHandler) MessageHandler: Тип функции, которая изменяет или расширяет обработку сообщений.

### Message

Структура Message представляет сообщение Kafka с дополнительной метаинформацией (например, топик, ключ, заголовки и т.д.).
```go
type Message struct {
    TopicPartition TopicPartition
    Value          []byte
    Key            Key
    Timestamp      time.Time
    TimestampType  TimestampType
    Headers        []Header
}
```
### Лицензия

Этот пакет лицензирован по лицензии MIT. См. файл LICENSE для подробностей.
