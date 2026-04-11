package middleware

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/overtonx/kafkalight"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/overtonx/kafkalight/middleware"
	instrumentationVersion = "v1.0.8"
)

func Tracing(consumerGroup ...string) kafkalight.Middleware {
	group := ""
	if len(consumerGroup) > 0 {
		group = consumerGroup[0]
	}
	tracer := otel.Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(instrumentationVersion),
	)

	return func(next kafkalight.MessageHandler) kafkalight.MessageHandler {
		spanName := handlerSpanName(next)
		return func(ctx context.Context, msg *kafkalight.Message) error {
			propagator := otel.GetTextMapPropagator()
			ctx = propagator.Extract(ctx, kafkalight.NewMessageCarrier(msg))

			ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindConsumer))
			span.SetAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.operation.type", "process"),
				attribute.String("messaging.destination.name", msg.TopicPartition.Topic),
				attribute.Int("messaging.kafka.destination.partition", int(msg.TopicPartition.Partition)),
				attribute.Int64("messaging.kafka.message.offset", msg.TopicPartition.Offset),
			)
			if group != "" {
				span.SetAttributes(attribute.String("messaging.consumer.group.name", group))
			}
			defer span.End()

			start := time.Now()
			err := next(ctx, msg)
			span.SetAttributes(attribute.Float64("messaging.process.duration_ms", float64(time.Since(start).Microseconds())/1000))
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			span.SetStatus(codes.Ok, "message processed")
			return nil
		}
	}
}

// handlerSpanName derives a "Struct.Method" span name from a MessageHandler
// function using runtime reflection. For plain functions it returns the
// function name; for method receivers it strips the package path and pointer
// notation, e.g. "(*Consumer).Handle" → "Consumer.Handle".
func handlerSpanName(h kafkalight.MessageHandler) string {
	full := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()
	if i := strings.LastIndex(full, "/"); i >= 0 {
		full = full[i+1:]
	}
	if i := strings.Index(full, "."); i >= 0 {
		full = full[i+1:]
	}
	full = strings.ReplaceAll(full, "(*", "")
	full = strings.ReplaceAll(full, ")", "")
	full = strings.TrimSuffix(full, "-fm")
	return full
}
