package descriptor

import (
	"context"
	"errors"
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/services"
)

type GrainDescription struct {
	GrainType   string
	Activation  ActivationDesc
	Methods     []MethodDesc
	Observables []ObservableDesc
}

type ActivationDesc struct {
	Handler        ActivationHandler
	DefaultTimeout time.Duration
}

type MethodDesc struct {
	Name           string
	Handler        MethodHandler
	DefaultTimeout time.Duration
}

type ObservableDesc struct {
	Name               string
	Handler            ObservableHandler
	RegisterHandler    RegisterObserverHandler
	UnsubscribeHandler UnsubscribeObserverHandler
	DefaultTimeout     time.Duration
}

type ActivationHandler func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, identity grain.Identity) (grain.GrainReference, error)
type MethodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error)
type ObservableHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) error
type RegisterObserverHandler func(srv interface{}, ctx context.Context, observer grain.Identity, registrationTimeout time.Duration, dec func(interface{}) error) error
type UnsubscribeObserverHandler func(srv interface{}, ctx context.Context, observer grain.Identity) error

var ErrGrainTypeNotFound = errors.New("grain type not found")

type Registrar interface {
	Register(desc *GrainDescription, impl interface{})
	Lookup(grainType string) (*GrainDescription, interface{}, error)
}
