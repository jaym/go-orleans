package silo

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

var identityCtxKey = struct{}{}

func IdentityFromContext(ctx context.Context) *grain.Identity {
	v, ok := ctx.Value(identityCtxKey).(grain.Identity)
	if ok {
		return &v
	}
	return nil
}

func WithIdentityContext(ctx context.Context, address grain.Identity) context.Context {
	return context.WithValue(ctx, identityCtxKey, address)
}
