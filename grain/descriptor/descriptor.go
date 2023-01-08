package descriptor

import (
	"context"
	"errors"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/services"
)

var ErrGrainTypeNotFound = errors.New("grain type not found")

type ActivatorConfig struct {
	ActivatorFunc      ActivatorFunc
	DefaultMailboxSize *int

	IsStatelessWorker bool
	MaxWorkers        *int
}

type ActivatorOpt func(*ActivatorConfig)

func WithDefaultMailboxSize(defaultMailboxSize int) ActivatorOpt {
	return func(ac *ActivatorConfig) {
		ac.DefaultMailboxSize = &defaultMailboxSize
	}
}

func WithStatelessWorker() ActivatorOpt {
	return func(ac *ActivatorConfig) {
		ac.IsStatelessWorker = true
	}
}

func WithMaxWorkers(maxWorkers int) ActivatorOpt {
	return func(ac *ActivatorConfig) {
		ac.MaxWorkers = &maxWorkers
	}
}

type Registrar interface {
	Register(grainType string, activatorFunc ActivatorFunc, opts ...ActivatorOpt)
	Lookup(grainType string) (ActivatorConfig, error)
}

type ActivatorFunc func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error)
