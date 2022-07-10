package timer

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
)

type GrainTimerTrigger func(grainAddr grain.Identity, name string, try int) bool

type TimerService interface {
	Start()
	Stop(context.Context) error
	RegisterTimer(ident grain.Identity, name string, d time.Duration) error
	RegisterTicker(ident grain.Identity, name string, d time.Duration) error
	Cancel(ident grain.Identity, name string) bool
}
