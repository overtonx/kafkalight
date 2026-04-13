# Журнал изменений

Все значимые изменения в проекте фиксируются в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
версионирование соответствует [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.0.9] - 2026-04-11

### Добавлено
- Middleware `Tracing()` теперь принимает опциональный параметр `consumerGroup` для установки атрибута span `messaging.consumer.group.name`.
- Middleware трейсинга теперь создаёт собственные span'ы со `SpanKindConsumer` вместо того, чтобы полагаться исключительно на извлечённый родительский span.
- Богатый набор атрибутов OpenTelemetry по спецификации messaging semantic conventions на каждом span'е:
  - `messaging.system` — `"kafka"`
  - `messaging.operation.type` — `"process"`
  - `messaging.destination.name` — имя топика
  - `messaging.kafka.destination.partition` — номер партиции
  - `messaging.kafka.message.offset` — offset сообщения
  - `messaging.consumer.group.name` — группа консьюмеров (если передана)
  - `messaging.process.duration_ms` — длительность обработки сообщения в миллисекундах
- Корректные статус-коды span'а: `codes.Ok` при успехе, `codes.Error` с `span.RecordError` при ошибке.
- Вспомогательная функция `handlerSpanName`, которая выводит читаемое имя span'а из `MessageHandler` через runtime-рефлексию (например, `Consumer.Handle`).
- Именованный instrumentation scope (`github.com/overtonx/kafkalight/middleware`) с версией `v1.0.9`.

### Изменено
- Тест трейсинга обновлён: `Tracing()` теперь вызывается с группой консьюмеров `"test-group"`.
- Изменена проверка в тесте трейсинга: ID дочернего span'а теперь должен отличаться от ID родительского (для каждого сообщения создаётся новый span).
- README.md дополнен документацией по управлению offset'ами (`enable.auto.commit`) с примерами автоматического и ручного коммита, а также пояснением семантики at-least-once.

### Обновлены зависимости
- Go: `1.24.0` → `1.25.0`
- `confluent-kafka-go/v2`: `v2.12.0` → `v2.13.3`
- `redis/go-redis/v9`: `v9.16.0` → `v9.18.0`
- `go.opentelemetry.io/otel`: `v1.38.0` → `v1.43.0`
- `go.opentelemetry.io/otel/sdk`: `v1.38.0` → `v1.42.0`
- `go.opentelemetry.io/otel/trace`: `v1.38.0` → `v1.43.0`
- `go.uber.org/zap`: `v1.27.0` → `v1.27.1`

### Удалено
- `github.com/go-sql-driver/mysql` и `github.com/overtonx/outbox/v2` удалены из прямых зависимостей.

## [v1.0.8] - 2026-03-29

### Исправлено
- Исправлены тесты авто-коммита.
- Исправлен метод `Close()` роутера: теперь он дожидается завершения горутины listener'а.

### Добавлено
- Коммит offset'а только после успешной обработки сообщения (режим `enable.auto.commit: false`).

## [v1.0.7] - 2026-03-29

### Изменено
- Ошибки по таймауту теперь пропускаются в обработчике ошибок.

## [v1.0.6] - 2026-03-29

### Добавлено
- Логирование всех ошибок Kafka через обработчик ошибок.

## [v1.0.5] - 2026-03-29

### Добавлено
- Роутер теперь поддерживает регистрацию нескольких middleware за один вызов.

## [v1.0.4] - 2026-03-29

### Добавлено
- Middleware для восстановления после паники (panic recovery).

### Изменено
- Скорректирована настройка listener'а.

## [v1.0.3] - 2026-03-29

### Изменено
- Middleware трейсинга рефакторингом переведён на использование дефолтного text-map propagator из OpenTelemetry.

## [v1.0.2] - 2026-03-29

### Изменено
- Обновлены зависимости для улучшения производительности и совместимости.

## [v1.0.0] - 2026-03-29

### Добавлено
- Обработка Kafka-сообщений с поддержкой middleware.
- Интеграция трейсинга через OpenTelemetry.

## [v0.0.1] - 2026-03-29

### Добавлено
- Первый релиз: middleware для обработки ошибок и логирования с интеграцией `zap`.

[v1.0.9]: https://github.com/overtonx/kafkalight/compare/v1.0.8...v1.0.9
[v1.0.8]: https://github.com/overtonx/kafkalight/compare/v1.0.7...v1.0.8
[v1.0.7]: https://github.com/overtonx/kafkalight/compare/v1.0.6...v1.0.7
[v1.0.6]: https://github.com/overtonx/kafkalight/compare/v1.0.5...v1.0.6
[v1.0.5]: https://github.com/overtonx/kafkalight/compare/v1.0.4...v1.0.5
[v1.0.4]: https://github.com/overtonx/kafkalight/compare/v1.0.3...v1.0.4
[v1.0.3]: https://github.com/overtonx/kafkalight/compare/v1.0.2...v1.0.3
[v1.0.2]: https://github.com/overtonx/kafkalight/compare/v1.0.0...v1.0.2
[v1.0.0]: https://github.com/overtonx/kafkalight/compare/v0.0.1...v1.0.0
[v0.0.1]: https://github.com/overtonx/kafkalight/releases/tag/v0.0.1
