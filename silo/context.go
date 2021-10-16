package silo

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

var addressCtxKey = struct{}{}
var siloClientCtxKey = struct{}{}

func AddressFromContext(ctx context.Context) *grain.Address {
	v, ok := ctx.Value(addressCtxKey).(grain.Address)
	if ok {
		return &v
	}
	return nil
}

func WithAddressContext(ctx context.Context, address grain.Address) context.Context {
	return context.WithValue(ctx, addressCtxKey, address)
}

func WithSiloClientContext(ctx context.Context, siloClient grain.SiloClient) context.Context {
	return context.WithValue(ctx, siloClientCtxKey, siloClient)
}

func SiloClientFromContext(ctx context.Context) (grain.SiloClient, bool) {
	v, ok := ctx.Value(addressCtxKey).(grain.SiloClient)
	return v, ok
}
