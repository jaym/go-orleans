package observer

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

type Store interface {
	List(ctx context.Context, owner grain.Address, observableName string) ([]grain.RegisteredObserver, error)
	Add(ctx context.Context, owner grain.Address, observableName string, address grain.Address, opts ...AddOption) error
	Remove(ctx context.Context, owner grain.Address, opts ...RemoveOption) error
}
