package cluster

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/jaym/go-orleans/grain"
)

var ErrGrainActivationNotFound = errors.New("grain activation not found")
var ErrGrainAlreadyActivated = errors.New("grain is already activated")
var ErrGrainDirectoryLockLost = errors.New("grain directory lock lost")

type GrainDirectoryLock interface {
	Activate(context.Context, grain.Identity) error
	Deactivate(context.Context, grain.Identity) error
	Lookup(context.Context, grain.Identity) (GrainAddress, error)
	Unlock(context.Context) error
	Err() error
}

type GrainDirectoryLockOnExpirationFunc func()

type GrainDirectory interface {
	Lock(context.Context, Location, GrainDirectoryLockOnExpirationFunc) (GrainDirectoryLock, error)
	Lookup(context.Context, grain.Identity) (GrainAddress, error)
}
