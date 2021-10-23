package observer

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

type Store interface {
	List(ctx context.Context, owner grain.Identity, observableName string) ([]grain.RegisteredObserver, error)
	Add(ctx context.Context, owner grain.Identity, observableName string, identity grain.Identity, opts ...AddOption) error
	Remove(ctx context.Context, owner grain.Identity, opts ...RemoveOption) error
}
