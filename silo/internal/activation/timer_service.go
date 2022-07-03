package activation

import (
	"context"
	"time"

	"github.com/jaym/go-orleans/grain"
	"github.com/jaym/go-orleans/silo/services/timer"
)

type timerInfo struct {
	f        func(ctx context.Context)
	isTicker bool
}

type grainTimerServiceImpl struct {
	grainIdentity grain.Identity
	timerService  timer.TimerService
	timers        map[string]timerInfo
}

func (g *grainTimerServiceImpl) RegisterTimer(name string, d time.Duration, f func(ctx context.Context)) error {
	if err := g.timerService.RegisterTimer(g.grainIdentity, name, d); err != nil {
		return err
	}
	g.timers[name] = timerInfo{f: f}
	return nil
}

func (g *grainTimerServiceImpl) RegisterTicker(name string, d time.Duration, f func(ctx context.Context)) error {
	if err := g.timerService.RegisterTicker(g.grainIdentity, name, d); err != nil {
		return err
	}
	g.timers[name] = timerInfo{f: f, isTicker: true}
	return nil
}

func (g *grainTimerServiceImpl) Trigger(ctx context.Context, name string) {
	if t, ok := g.timers[name]; ok {
		if !t.isTicker {
			delete(g.timers, name)
		}
		t.f(ctx)
	}
}

func (g *grainTimerServiceImpl) Cancel(name string) bool {
	if _, ok := g.timers[name]; ok {
		delete(g.timers, name)
		return g.timerService.Cancel(g.grainIdentity, name)
	}
	return false
}
