package rabbit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric"
)

type HandlerFunc func(context.Context, []byte) error
type SetupFunc func(ch *amqp091.Channel) error

type ConsumerConfig func(*Consumer) error

type Consumer struct {
	name           string
	queueName      string
	delivery       <-chan amqp091.Delivery
	logger         *slog.Logger
	handlers       map[string]HandlerFunc
	msgsQueue      chan amqp091.Delivery
	queueLength    int
	workersCount   int
	queue          *amqp091.Queue
	consumeCounter metric.Int64Counter
	successCounter metric.Int64Counter
	failCounter    metric.Int64Counter
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

	return nil
}

func (c *Consumer) RegisterHandler(routingKey string, handler HandlerFunc) {
	c.handlers[routingKey] = handler
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

	for msg := range c.msgsQueue {
		func() {
			lg.Info("rabbit message received in msg queue go channel", slog.String("routingKey", msg.RoutingKey))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*55)
			defer cancel()
			if c.consumeCounter != nil {
				c.consumeCounter.Add(ctx, 1)
			}
			handler, ok := c.handlers[msg.RoutingKey]
			if !ok {
				lg.Warn("no handler found for routingKey", slog.String("routingKey", msg.RoutingKey))
				if c.failCounter != nil {
					c.failCounter.Add(ctx, 1)
				}
				if err := msg.Ack(false); err != nil {
					lg.Error("failed to ack message", slog.Any("error", err))
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
					}
					lg.Info("rabbit message acked", slog.String("routingKey", msg.RoutingKey))
					return
				} else {
					if c.failCounter != nil {
						c.failCounter.Add(ctx, 1)
					}
					if err := msg.Nack(false, true); err != nil {
						lg.Error("failed to nack message", slog.Any("error", err))
						return
					}
					lg.Warn("rabbit message nacked", slog.String("routingKey", msg.RoutingKey))
				}
			}

		}()
	}
}
