package silo

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	grainservices "github.com/jaym/go-orleans/grain/services"
)

var grain_GrainDesc = GrainDescription{
	GrainType: "Grain",
	Activation: ActivationDesc{
		Handler: func(activator interface{}, ctx context.Context, coreServices grainservices.CoreGrainServices, o grainservices.GrainObserverManager, address grain.Address) (grain.Addressable, error) {
			return activator.(GenericGrainActivator).Activate(ctx, address)
		},
	},
}

type GenericGrainActivator interface {
	Activate(context.Context, grain.Address) (grain.Addressable, error)
}

type coreGrainService struct {
	grainTimerServices grainservices.GrainTimerService
	siloClient         grain.SiloClient
}

func (c *coreGrainService) TimerService() grainservices.GrainTimerService {
	return c.grainTimerServices
}

func (c *coreGrainService) SiloClient() grain.SiloClient {
	return c.siloClient
}

type HasCanEvict interface {
	CanEvict(ctx context.Context) bool
}

type HasDeactivate interface {
	Deactivate(ctx context.Context)
}
