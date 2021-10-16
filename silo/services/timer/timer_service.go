package timer

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
)

type GrainTimerTrigger func(grainAddr grain.Address, name string)

type TimerService interface {
	Start()
	Stop(context.Context) error
	RegisterTimer(addr grain.Address, name string, d time.Duration) error
	RegisterTicker(addr grain.Address, name string, d time.Duration) error
	Cancel(addr grain.Address, name string) bool
}