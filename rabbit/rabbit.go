package rabbit

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type ConsumerRunner struct {
	consumers []*Consumer
	lg        *slog.Logger
	channel   *amqp091.Channel
}

func NewConsumerRunner(lg *slog.Logger, consumers ...*Consumer) *ConsumerRunner {
	return &ConsumerRunner{
		consumers: consumers,
		lg:        lg,
	}
}

func (cr *ConsumerRunner) RunInnerWorkers() {
	for _, c := range cr.consumers {
		c.RunInnerWorkers()
	}
}

func (cr *ConsumerRunner) Setup(ch *amqp091.Channel) error {
	cr.channel = ch

	for _, consumer := range cr.consumers {
		if err := consumer.Setup(ch); err != nil {
			cr.lg.Error("failed to setup consumer", "err", err)
			return err
		}
	}
	go cr.RunInnerWorkers()
	return cr.startWorkers()
}

func (cr *ConsumerRunner) startWorkers() error {
	for _, consumer := range cr.consumers {
		go consumer.Worker()
	}
	return nil
}

type Rabbit struct {
	CH               *amqp091.Channel
	conn             *amqp091.Connection
	eventExchange    string
	internalExchange string
	connectionString string
	lg               *slog.Logger
	connected        bool
	Mutex            sync.RWMutex
}

func NewRabbit(connectionString string, lg *slog.Logger) *Rabbit {
	return &Rabbit{
		connectionString: connectionString,
		lg:               lg,
	}
}

func (r *Rabbit) Setup(consumerRunner *ConsumerRunner) error {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	if err := r.cleanup(); err != nil {
		return err
	}

	if err := r.Connect(); err != nil {
		return err
	}

	if err := r.SetupChannel(); err != nil {
		return err
	}
	if err := consumerRunner.Setup(r.CH); err != nil {
		return err
	}

	r.connected = true
	go r.handleDisconnect(consumerRunner)

	return nil
}

func (r *Rabbit) cleanup() error {
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			r.lg.Error("failed to close connection", "err", err)
		}
	}
	if r.CH != nil {
		if err := r.CH.Close(); err != nil {
			r.lg.Error("failed to close channel", "err", err)
		}
	}
	return nil
}

func (r *Rabbit) Connect() error {
	conn, err := amqp091.Dial(r.connectionString)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}

func (r *Rabbit) SetupChannel() error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	r.CH = ch

	if err := r.CH.Qos(8, 0, false); err != nil {
		return err
	}

	return nil
}

func (r *Rabbit) handleDisconnect(consumerRunner *ConsumerRunner) {
	closeChan := make(chan *amqp091.Error)
	r.conn.NotifyClose(closeChan)

	err := <-closeChan
	r.connected = false
	r.lg.Error("RabbitMQ connection closed", "err", err)

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for !r.connected {
		r.lg.Info("attempting to reconnect to RabbitMQ", "backoff", backoff)
		time.Sleep(backoff)

		if err := r.Setup(consumerRunner); err != nil {
			r.lg.Error("failed to reconnect", "err", err)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		r.lg.Info("successfully reconnected to RabbitMQ")
		return
	}
}

func (r *Rabbit) HealthCheck() error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()

	if r.conn == nil || r.conn.IsClosed() {
		return errors.New("rabbitmq connection is not open")
	}

	if r.CH == nil || r.CH.IsClosed() {
		return errors.New("rabbitmq channel is not open")
	}

	return nil
}
