# amqp-consumer

Strong rabbitmq consumer runner designed for simplicity of using, stability and performance

## Usage
```go
    logger := slog.Default()

    // Create rabbitMQ object
	rabbitMQ := rabbit.NewRabbit("amqp://guest:guest@localhost:5672/", logger)

    // Create a consumer
	consumer, err := rabbit.NewConsumer(logger, 1, 1, "queue_name", "consume_name",
        rabbit.WithOtelMetric(meter), // otel meter
    )
	if err != nil {
		panic(err)
	}

    // Register handler for the consumer based on routingKey
	consumer.RegisterHandler("order.created", func(ctx context.Context, msg []byte) error {
		logger.Info("Processing order:", slog.String("data", string(msg)))
		return nil
	})
    
    // Create consumers that is consists of multiple consumer
	consumerRunner := rabbit.NewConsumerRunner(logger, consumer, consumer2, ...)

	if err := rabbitMQ.Setup(consumerRunner); err != nil {
		logger.Error("Failed to setup RabbitMQ", slog.Any("error", err))
		return
	}

```

