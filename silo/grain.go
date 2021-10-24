package silo

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/descriptor"
	grainservices "github.com/jaym/go-orleans/grain/services"
)

var grain_GrainDesc = descriptor.GrainDescription{
	GrainType: "Grain",
	Activation: descriptor.ActivationDesc{
		Handler: func(activator interface{}, ctx context.Context, coreServices grainservices.CoreGrainServices, o grainservices.GrainObserverManager, identity grain.Identity) (grain.GrainReference, error) {
			return activator.(GenericGrainActivator).Activate(ctx, identity)
		},
	},
}

type GenericGrainActivator interface {
	Activate(context.Context, grain.Identity) (grain.GrainReference, error)
}
