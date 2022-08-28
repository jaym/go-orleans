package descriptor

import (
	"context"
	"errors"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/services"
)

var ErrGrainTypeNotFound = errors.New("grain type not found")

type Registrar interface {
	RegisterV2(grainType string, activatorFunc ActivatorFunc)
	LookupV2(grainType string) (ActivatorFunc, error)
}

type ActivatorFunc func(ctx context.Context, identity grain.Identity, services services.CoreGrainServices) (grain.Activation, error)
