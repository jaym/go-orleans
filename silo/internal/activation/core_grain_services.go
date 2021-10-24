package activation

import (
	"context"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/grain/services"
)

type coreGrainService struct {
	grainTimerServices services.GrainTimerService
	siloClient         grain.SiloClient
}

func (c *coreGrainService) TimerService() services.GrainTimerService {
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
