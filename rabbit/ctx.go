package rabbit

import "context"

type ctxKey string

const (
	routingKeyCtxKey   ctxKey = "routingKey"
	exchangeNameCtxKey ctxKey = "exchangeName"
)

// SetRoutingInfo adds routingKey and exchangeName to context.
func SetRoutingInfo(ctx context.Context, routingKey, exchangeName string) context.Context {
	ctx = context.WithValue(ctx, routingKeyCtxKey, routingKey)
	ctx = context.WithValue(ctx, exchangeNameCtxKey, exchangeName)
	return ctx
}

// GetRoutingKey extracts routingKey from context.
func GetRoutingKey(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(routingKeyCtxKey).(string)
	return v, ok
}

// GetExchangeName extracts exchangeName from context.
func GetExchangeName(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(exchangeNameCtxKey).(string)
	return v, ok
}
