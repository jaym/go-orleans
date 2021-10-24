package descriptor

import (
	"context"

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
	Handler ActivationHandler
}

type MethodDesc struct {
	Name    string
	Handler MethodHandler
}

type ObservableDesc struct {
	Name            string
	Handler         ObservableHandler
	RegisterHandler RegisterObserverHandler
}

type ActivationHandler func(activator interface{}, ctx context.Context, coreServices services.CoreGrainServices, o services.GrainObserverManager, identity grain.Identity) (grain.GrainReference, error)
type MethodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error)
type ObservableHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error) error
type RegisterObserverHandler func(srv interface{}, ctx context.Context, observer grain.Identity, dec func(interface{}) error) error

type Registrar interface {
	Register(desc *GrainDescription, impl interface{})
	Lookup(grainType string) (*GrainDescription, interface{}, error)
}
