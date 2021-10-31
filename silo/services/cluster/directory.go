package cluster

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/jaym/go-orleans/grain"
)

var ErrGrainActivationNotFound = errors.New("grain activation not found")
var ErrGrainAlreadyActivated = errors.New("grain is already activated")

type GrainDirectory interface {
	Lookup(context.Context, grain.Identity) (GrainAddress, error)
	Activate(context.Context, GrainAddress) error
	Deactivate(context.Context, GrainAddress) error
}
