package silo

import "context"

var addressCtxKey = struct{}{}
var siloClientCtxKey = struct{}{}

func AddressFromContext(ctx context.Context) *Address {
	v, ok := ctx.Value(addressCtxKey).(Address)
	if ok {
		return &v
	}
	return nil
}

func WithAddressContext(ctx context.Context, address Address) context.Context {
	return context.WithValue(ctx, addressCtxKey, address)
}

func WithSiloClientContext(ctx context.Context, siloClient SiloClient) context.Context {
	return context.WithValue(ctx, siloClientCtxKey, siloClient)
}

func SiloClientFromContext(ctx context.Context) (SiloClient, bool) {
	v, ok := ctx.Value(addressCtxKey).(SiloClient)
	return v, ok
}
