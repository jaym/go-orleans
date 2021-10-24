package activation

import (
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type grainTimerServiceImpl struct {
	grainIdentity grain.Identity
	timerService  timer.TimerService
	timers        map[string]func()
}

func (g *grainTimerServiceImpl) RegisterTimer(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTimer(g.grainIdentity, name, d); err != nil {
		return err
	}
	g.timers[name] = f
	return nil
}

func (g *grainTimerServiceImpl) RegisterTicker(name string, d time.Duration, f func()) error {
	if err := g.timerService.RegisterTicker(g.grainIdentity, name, d); err != nil {
		return err
	}
	g.timers[name] = f
	return nil
}

func (g *grainTimerServiceImpl) Trigger(name string) {
	if f, ok := g.timers[name]; ok {
		delete(g.timers, name)
		f()
	}
}

func (g *grainTimerServiceImpl) Cancel(name string) bool {
	if _, ok := g.timers[name]; ok {
		delete(g.timers, name)
		return g.timerService.Cancel(g.grainIdentity, name)
	}
	return false
}
