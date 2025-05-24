package rabbit

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type HandlerFunc func(context.Context, []byte) error

type RegexHandler struct {
	Pattern *regexp.Regexp
	Handler HandlerFunc
}

type SetupFunc func(ch *amqp091.Channel) error

type ConsumerConfig func(*Consumer) error

type Consumer struct {
	name           string
	queueName      string
	delivery       <-chan amqp091.Delivery
	logger         *slog.Logger
	handlers       map[string]HandlerFunc
	regexHandlers  []RegexHandler
	msgsQueue      chan amqp091.Delivery
	queueLength    int
	workersCount   int
	queue          *amqp091.Queue
	consumeCounter metric.Int64Counter
	successCounter metric.Int64Counter
	failCounter    metric.Int64Counter
	tracer         trace.Tracer
}

func WithQueue(queue amqp091.Queue) ConsumerConfig {
	return func(c *Consumer) error {
		c.queue = &queue
		return nil
	}
}

func WithHandlers(handlers map[string]HandlerFunc) ConsumerConfig {
	return func(c *Consumer) error {
		c.handlers = handlers
		return nil
	}
}

func WithOtelMetric(meter metric.Meter) ConsumerConfig {
	return func(c *Consumer) error {
		var err error

		c.consumeCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.total.counter", c.name))
		if err != nil {
			return err
		}

		c.successCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.success.counter", c.name))
		if err != nil {
			return err
		}

		c.failCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.failed.counter", c.name))
		if err != nil {
			return err
		}

		return nil
	}
}

// Add a configuration function to set the tracer
func WithTracer(tracer trace.Tracer) ConsumerConfig {
	return func(c *Consumer) error {
		c.tracer = tracer
		return nil
	}
}

type Carrier amqp091.Table

func (c Carrier) Get(key string) string {
	v := c[key]
	switch v := v.(type) {
	case nil:
		return ""
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
func (c Carrier) Set(key string, value string) {
	c[key] = value
}

func (c Carrier) Keys() []string {
	keys := []string{}
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func NewConsumer(
	l *slog.Logger,
	queueLength int,
	workersCount int,
	queueName string,
	consumerName string,
	configs ...ConsumerConfig,
) (*Consumer, error) {
	ec := &Consumer{
		name:         consumerName,
		queueName:    queueName,
		logger:       l.With("layer", "Consumer"),
		handlers:     map[string]HandlerFunc{},
		queueLength:  queueLength,
		workersCount: workersCount,
		msgsQueue:    make(chan amqp091.Delivery),
	}

	for _, cfg := range configs {
		err := cfg(ec)
		if err != nil {
			return nil, err
		}
	}

	return ec, nil
}

func (c *Consumer) RunInnerWorkers() {
	for i := 0; i < c.workersCount; i++ {
		go c.innerWorker()
	}
}

func (c *Consumer) Setup(ch *amqp091.Channel) error {

	d, err := ch.Consume(c.queueName, c.name, false,
		false, false, false, nil)
	if err != nil {
		return err
	}
	c.delivery = d
	if c.tracer == nil {
		c.tracer = otel.Tracer("rabbitmq/consumer")
	}
	return nil
}

func rabbitMQWildcardToRegex(pattern string) (*regexp.Regexp, error) {
	word := `[^.]+`

	// Special case: pattern is exactly "#"
	if pattern == "#" {
		// matches zero or more words separated by dots (including empty)
		return regexp.Compile(`^(` + word + `(\.` + word + `)*)?$`)
	}

	// Escape dots first
	pattern = regexp.QuoteMeta(pattern)

	// Replace * with single word wildcard (no dots)
	pattern = strings.ReplaceAll(pattern, `\*`, `(`+word+`)`)

	// Replace .# with zero or more words preceded by dot
	pattern = strings.ReplaceAll(pattern, `\.#`, `(\.`+word+`)*`)

	// Replace #. with zero or more words followed by dot
	pattern = strings.ReplaceAll(pattern, `#\.`, `(`+word+`\.)*`)

	// Replace remaining # with zero or more words with optional dot
	pattern = strings.ReplaceAll(pattern, `#`, `(`+word+`(\.`+word+`)*)?`)

	return regexp.Compile("^" + pattern + "$")
}

func (c *Consumer) RegisterHandler(routingKey string, handler HandlerFunc) {
	c.handlers[routingKey] = handler
}

// RegisterRegexHandler will register a handler in form of rabbitmq wildcard
func (c *Consumer) RegisterRegexHandler(pattern string, handler HandlerFunc) error {
	re, err := rabbitMQWildcardToRegex(pattern)
	if err != nil {
		return err
	}
	c.regexHandlers = append(c.regexHandlers, RegexHandler{Pattern: re, Handler: handler})
	return nil
}

func (c *Consumer) Worker() {
	lg := c.logger.With("method", "Worker")
	lg.Info("started Consumer worker")
	for msg := range c.delivery {
		c.msgsQueue <- msg
	}
}

func (c *Consumer) innerWorker() {
	lg := c.logger.With("method", "InnerWorker")
	lg.Info("started Consumer inner worker")

	propagator := otel.GetTextMapPropagator()
	for msg := range c.msgsQueue {
		func() {
			lg.Info("rabbit message received in msg queue go channel", slog.String("routingKey", msg.RoutingKey))

			// Extract the context from the message headers
			ctx := context.Background()
			eCtx := propagator.Extract(ctx, Carrier(msg.Headers))
			spanCtx := trace.SpanContextFromContext(eCtx)
			bags := baggage.FromContext(eCtx)
			ctx = baggage.ContextWithBaggage(ctx, bags)
			ctx, span := c.tracer.Start(
				trace.ContextWithRemoteSpanContext(ctx, spanCtx),
				"rabbitmq_message",
				trace.WithSpanKind(trace.SpanKindConsumer),
			)

			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					lg.Error("recovered from panic",
						"panic", r,
						"stack", string(stack),
					)

					if err := msg.Nack(false, true); err != nil {
						lg.Error("failed to ack message during panic recovery", slog.Any("error", err))
						recordTraceError(err, span)
					} else {
						lg.Info("message acked during panic recovery", slog.String("routingKey", msg.RoutingKey))
					}
				}
			}()

			defer span.End()

			ctx, cancel := context.WithTimeout(ctx, time.Second*55)
			defer cancel()

			if c.consumeCounter != nil {
				c.consumeCounter.Add(ctx, 1)
			}

			handler, ok := c.handlers[msg.RoutingKey]
			if !ok {
				for _, rh := range c.regexHandlers {
					if rh.Pattern.MatchString(msg.RoutingKey) {
						handler = rh.Handler
						ok = true
						break
					}
				}
			}

			if !ok {
				lg.Warn("no handler found for routingKey", slog.String("routingKey", msg.RoutingKey))
				if c.failCounter != nil {
					c.failCounter.Add(ctx, 1)
				}
				if err := msg.Ack(false); err != nil {
					lg.Error("failed to ack message", slog.Any("error", err))
					recordTraceError(err, span)
				}
				lg.Warn("rabbit message acked(no handler found)", slog.String("routingKey", msg.RoutingKey))
				return
			} else {
				if err := handler(ctx, msg.Body); err == nil {
					if c.successCounter != nil {
						c.successCounter.Add(ctx, 1)
					}
					if err := msg.Ack(false); err != nil {
						lg.Error("failed to ack message", slog.Any("error", err))
						recordTraceError(err, span)
					}
					lg.Info("rabbit message acked", slog.String("routingKey", msg.RoutingKey))
					return
				} else {
					if c.failCounter != nil {
						c.failCounter.Add(ctx, 1)
					}
					if err := msg.Nack(false, true); err != nil {
						lg.Error("failed to nack message", slog.Any("error", err))
						recordTraceError(err, span)
						return
					}
					lg.Warn("rabbit message nacked", slog.String("routingKey", msg.RoutingKey))
				}
			}

		}()
	}
}

func recordTraceError(err error, span trace.Span) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
