package timer

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
)

type GrainTimerTrigger func(grainAddr grain.Identity, name string)

type TimerService interface {
	Start()
	Stop(context.Context) error
	RegisterTimer(addr grain.Identity, name string, d time.Duration) error
	RegisterTicker(addr grain.Identity, name string, d time.Duration) error
	Cancel(addr grain.Identity, name string) bool
}
