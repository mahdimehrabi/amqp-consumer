package rabbit

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"log/slog"
	"time"
)

type HandlerFunc func(context.Context, []byte) error
type SetupFunc func(ch *amqp091.Channel) error

type Consumer struct {
	q            *amqp091.Queue
	delivery     <-chan amqp091.Delivery
	logger       *slog.Logger
	handlers     map[string]HandlerFunc
	msgsQueue    chan amqp091.Delivery
	queueLength  int
	workersCount int
	queue        *amqp091.Queue
}

func NewRetryConsumer(l *slog.Logger, queueLength int, workersCount int, queue *amqp091.Queue, exchangeName, consumerName string) *Consumer {
	ec := &Consumer{logger: l.With("layer", "Consumer"), handlers: map[string]HandlerFunc{},
		queueLength: queueLength, workersCount: workersCount, msgsQueue: make(chan amqp091.Delivery), queue: queue}

	return ec
}

func (c *Consumer) RunInnerWorkers() {
	for i := 0; i < c.workersCount; i++ {
		go c.innerWorker()
	}
}

func (c *Consumer) Setup(ch *amqp091.Channel) error {
	d, err := ch.Consume(c.q.Name, "", false,
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
	lg := c.logger.With("method", "Worker")
	lg.Info("started Consumer inner worker")

	for msg := range c.msgsQueue {
		lg.Info("rabbit message received in msg queue go channel", slog.String("routingKey", msg.RoutingKey))
		handler, ok := c.handlers[msg.RoutingKey]
		if !ok {
			lg.Warn("no handler found for routingKey", slog.String("routingKey", msg.RoutingKey))
			if err := msg.Ack(false); err != nil {
				lg.Error("failed to ack message", slog.Any("error", err))
			}
			lg.Warn("rabbit message acked(no handler found)", slog.String("routingKey", msg.RoutingKey))
			continue
		} else {
			ctx, _ := context.WithTimeout(context.Background(), time.Second*55)
			if err := handler(ctx, msg.Body); err == nil {
				if err := msg.Ack(false); err != nil {
					lg.Error("failed to ack message", slog.Any("error", err))
				}
				lg.Info("rabbit message acked", slog.String("routingKey", msg.RoutingKey))
				continue
			}
		}

		if err := msg.Nack(false, true); err != nil {
			lg.Error("failed to nack message", slog.Any("error", err))
			continue
		}
		lg.Warn("rabbit message nacked", slog.String("routingKey", msg.RoutingKey))
	}
}
