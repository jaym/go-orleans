package cluster

import (
	"context"

	"github.com/jaym/go-orleans/grain"
)

type GrainDirectory interface {
	Lookup(context.Context, grain.Identity) (GrainAddress, error)
	Activate(context.Context, grain.Identity) error
	Deactivate(context.Context, grain.Identity) error
}
